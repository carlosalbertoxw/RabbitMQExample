using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReliableMessaging.Consumer;

internal sealed class OrderProcessor(
    ConnectionFactory factory,
    PaymentService payments,
    IHostApplicationLifetime lifetime,
    ILogger<OrderProcessor> logger) : BackgroundService
{
    private const int MaxRetries = 3;
    private const string RetryHeader = "x-retry-count";

    // Cuántos mensajes sin confirmar puede tener este Consumer a la vez. Un valor moderado evita
    // viajes de ida y vuelta por cada mensaje sin acaparar la cola si hay varios Consumers.
    private const ushort PrefetchCount = 10;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    // Ids de los pedidos ya cobrados, para no cobrar dos veces si un mensaje llega repetido.
    // Se guarda en memoria para no complicar el ejemplo; en producción iría en la misma base de datos
    // y transacción que el resultado del procesamiento, para que sobreviva a los reinicios.
    private readonly HashSet<string> processed = [];

    // Los mensajes se procesan de uno en uno; el semáforo permite esperar al que está en curso al apagar.
    private readonly SemaphoreSlim gate = new(1, 1);
    private volatile bool stopping;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await RabbitMqConnection.ConnectAsync(factory, logger, stoppingToken);

            // Confirmaciones también aquí: al mandar un pedido a la cola de reintentos hay que saber que llegó
            // antes de confirmar (ack) el original; si no, se podría perder entre las dos operaciones.
            await using var channel = await connection.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                stoppingToken);

            await Topology.DeclareAsync(channel, stoppingToken);
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: PrefetchCount, global: false, stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, ea) => HandleAsync(channel, ea);
            var consumerTag = await channel.BasicConsumeAsync(Topology.Queue, autoAck: false, consumer, stoppingToken);

            logger.LogInformation("Procesando pedidos de la cola \"{Queue}\". Ctrl+C para detener", Topology.Queue);

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Ctrl+C o SIGTERM: empieza el apagado ordenado.
            }

            logger.LogInformation("Apagando: dejo de recibir pedidos y termino el que está en curso");
            stopping = true;
            await channel.BasicCancelAsync(consumerTag);
            await gate.WaitAsync();
            gate.Release();
            logger.LogInformation("Apagado completo. Los pedidos recibidos sin confirmar vuelven a la cola");
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Se pidió detener mientras todavía se estaba conectando.
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "El procesador se detuvo por un error");
            Environment.ExitCode = 1;
            lifetime.StopApplication();
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea)
    {
        await gate.WaitAsync();
        try
        {
            if (stopping)
            {
                return; // sin ack: al cerrar el canal, RabbitMQ lo devuelve a la cola
            }

            var messageId = ea.BasicProperties.MessageId ?? $"sin-id-{ea.DeliveryTag}";
            using (logger.BeginScope("Mensaje {MessageId}", messageId))
            {
                await ProcessAsync(channel, ea, messageId);
            }
        }
        catch (Exception ex)
        {
            // Error inesperado (p. ej. se cayó la conexión a mitad del proceso). Se devuelve el mensaje a la cola;
            // si vuelve a fallar una y otra vez, x-delivery-limit acaba enviándolo a "orders.dead".
            logger.LogError(ex, "Error inesperado con la entrega {DeliveryTag}; se reencola", ea.DeliveryTag);
            try
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
            }
            catch (Exception nackError)
            {
                // Si el canal ya se cerró, RabbitMQ reencola el mensaje por su cuenta.
                logger.LogDebug(nackError, "No se pudo hacer nack");
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task ProcessAsync(IChannel channel, BasicDeliverEventArgs ea, string messageId)
    {
        // 1. Deserializar y validar. Un mensaje mal formado no se arregla reintentando: va directo a la cola de muertos.
        Order? order;
        try
        {
            order = JsonSerializer.Deserialize<Order>(ea.Body.Span, JsonOptions);
        }
        catch (JsonException ex)
        {
            await RejectAsync(channel, ea, $"JSON inválido: {ex.Message}");
            return;
        }

        if (Validate(order) is { } validationError)
        {
            await RejectAsync(channel, ea, validationError);
            return;
        }

        // 2. Idempotencia: RabbitMQ garantiza "al menos una vez", así que un mensaje puede llegar repetido.
        if (processed.Contains(messageId))
        {
            logger.LogWarning("Pedido #{Number} duplicado: ya estaba cobrado, se confirma sin cobrar otra vez", order!.Number);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            return;
        }

        // 3. Procesar, con reintentos diferidos para los errores pasajeros.
        var retry = GetRetryCount(ea.BasicProperties);
        try
        {
            await payments.ChargeAsync(order!, retry);
        }
        catch (TransientPaymentException ex) when (retry < MaxRetries)
        {
            await ScheduleRetryAsync(channel, ea, retry + 1);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            logger.LogWarning("Pedido #{Number}: {Error}. Reintento {Retry}/{Max} en {Delay} s",
                order!.Number, ex.Message, retry + 1, MaxRetries, Topology.RetryDelay.TotalSeconds);
            return;
        }
        catch (TransientPaymentException ex)
        {
            await RejectAsync(channel, ea, $"{ex.Message}; se agotaron los {MaxRetries} reintentos");
            return;
        }

        // Se marca como procesado antes del ack: si el ack se pierde y el mensaje vuelve, no se cobra dos veces.
        processed.Add(messageId);
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

        var afterRetries = retry > 0 ? $" tras {retry} reintento(s)" : string.Empty;
        var redelivered = ea.Redelivered ? " (reentregado)" : string.Empty;
        logger.LogInformation("Pedido #{Number} procesado: cobro de {Total:0.00} a {Customer}{AfterRetries}{Redelivered}",
            order!.Number, order.Total, order.Customer, afterRetries, redelivered);
    }

    private static string? Validate(Order? order) => order switch
    {
        null => "el mensaje está vacío",
        _ when order.Id == Guid.Empty => "el pedido no tiene Id",
        _ when string.IsNullOrWhiteSpace(order.Customer) => "el pedido no tiene cliente",
        _ when order.Total <= 0 => $"el total debe ser mayor que cero y llegó {order.Total:0.00}",
        _ => null
    };

    // nack sin reencolar: la cola "orders" lo envía a "orders.dead" (x-dead-letter-exchange), donde queda
    // para revisarlo a mano. RabbitMQ le agrega la cabecera x-death con el motivo y la cola de origen.
    private async Task RejectAsync(IChannel channel, BasicDeliverEventArgs ea, string reason)
    {
        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        logger.LogError("Mensaje enviado a \"{DeadLetterQueue}\": {Reason}", Topology.DeadLetterQueue, reason);
    }

    // Publica una copia en la cola de espera con el contador de reintentos actualizado. Al vencer su TTL,
    // RabbitMQ la devuelve a la cola principal. Con las confirmaciones activas, este await termina cuando
    // la copia ya está a salvo, así que es seguro hacer ack del original después.
    private static async Task ScheduleRetryAsync(IChannel channel, BasicDeliverEventArgs ea, int retry)
    {
        var headers = new Dictionary<string, object?>(ea.BasicProperties.Headers ?? new Dictionary<string, object?>())
        {
            [RetryHeader] = retry
        };
        var properties = new BasicProperties(ea.BasicProperties) { Headers = headers };

        await channel.BasicPublishAsync(string.Empty, Topology.RetryQueue, mandatory: true, properties, ea.Body);
    }

    private static int GetRetryCount(IReadOnlyBasicProperties properties) =>
        properties.Headers?.TryGetValue(RetryHeader, out var value) == true && value is not null
            ? Convert.ToInt32(value)
            : 0;
}
