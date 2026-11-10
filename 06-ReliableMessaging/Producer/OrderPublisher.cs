using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace ReliableMessaging.Producer;

internal sealed class OrderPublisher(
    ConnectionFactory factory,
    IConfiguration configuration,
    IHostApplicationLifetime lifetime,
    ILogger<OrderPublisher> logger) : BackgroundService
{
    // 6 intentos con espera exponencial (1, 2, 4, 8 y 16 s) suman 31 s: tiempo de sobra para que la
    // recuperación automática reconecte tras un reinicio del broker (lo intenta cada 5 s).
    private const int MaxPublishAttempts = 6;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var count = configuration.GetValue("count", 0);
        var interval = TimeSpan.FromMilliseconds(configuration.GetValue("interval", 1000));

        try
        {
            await using var connection = await RabbitMqConnection.ConnectAsync(factory, logger, stoppingToken);

            // Confirmaciones del publicador: cada BasicPublishAsync espera a que el broker confirme que guardó
            // el mensaje y lanza una excepción si lo rechaza o si no encuentra a dónde enrutarlo.
            await using var channel = await connection.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                stoppingToken);

            await Topology.DeclareAsync(channel, stoppingToken);

            if (count > 0)
            {
                logger.LogInformation("Publicando {Count} pedidos", count);
            }
            else
            {
                logger.LogInformation("Publicando pedidos cada {Interval} ms hasta Ctrl+C", interval.TotalMilliseconds);
            }

            for (var number = 1; count == 0 || number <= count; number++)
            {
                await PublishAsync(channel, CreateOrder(number), stoppingToken);

                if (count == 0 || number < count)
                {
                    await Task.Delay(interval, stoppingToken);
                }
            }

            logger.LogInformation("Listo: el broker confirmó todos los pedidos");
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Publicación detenida");
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "El publicador se detuvo por un error");
            Environment.ExitCode = 1;
        }
        finally
        {
            lifetime.StopApplication();
        }
    }

    private async Task PublishAsync(IChannel channel, Order order, CancellationToken cancellationToken)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(order, JsonOptions);
        var properties = new BasicProperties
        {
            // Persistente + cola quorum + confirmación = el pedido ya está a salvo en el broker cuando
            // BasicPublishAsync termina.
            Persistent = true,
            // El MessageId se repite en cada reintento de publicación; el Consumer lo usa para descartar duplicados.
            MessageId = order.Id.ToString(),
            ContentType = "application/json"
        };

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                // mandatory: true -> si ninguna cola está enlazada con esta routing key, el broker devuelve
                // el mensaje (PublishReturnException) en lugar de descartarlo en silencio.
                await channel.BasicPublishAsync(Topology.Exchange, Topology.RoutingKey, mandatory: true,
                    properties, body, cancellationToken);

                logger.LogInformation("Pedido #{Number} confirmado por el broker ({Customer}, total {Total:0.00})",
                    order.Number, order.Customer, order.Total);
                return;
            }
            catch (PublishReturnException ex)
            {
                // Es un problema de topología, no algo pasajero: reintentar no lo arregla.
                logger.LogError("Pedido #{Number} sin ruta ({ReplyText}): ninguna cola recibe \"{RoutingKey}\"",
                    order.Number, ex.ReplyText, Topology.RoutingKey);
                throw;
            }
            catch (Exception ex) when (ex is PublishException or AlreadyClosedException && attempt < MaxPublishAttempts)
            {
                // El broker lo rechazó (nack) o se cayó la conexión antes de confirmarlo. Se publica otra vez con
                // el mismo MessageId: si el primer intento sí había llegado, el Consumer descarta el duplicado.
                var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
                logger.LogWarning("Pedido #{Number} sin confirmar ({Error}). Reintento {Attempt}/{Max} en {Delay} s",
                    order.Number, ex.Message, attempt, MaxPublishAttempts - 1, delay.TotalSeconds);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    // Algunos números generan casos especiales a propósito, para recorrer cada camino del Consumer:
    //   múltiplos de 7  -> total negativo: inválido, va directo a "orders.dead" sin reintentos
    //   múltiplos de 11 -> "cliente-sin-servicio": el cobro falla siempre; tras 3 reintentos va a "orders.dead"
    //   múltiplos de 5  -> "cliente-inestable": el cobro falla 2 veces y se procesa en el tercer intento
    private static Order CreateOrder(int number)
    {
        var total = Math.Round((decimal)(Random.Shared.NextDouble() * 490 + 10), 2);
        var customer = $"cliente-{number % 3 + 1}";

        if (number % 7 == 0)
        {
            total = -total;
        }
        else if (number % 11 == 0)
        {
            customer = "cliente-sin-servicio";
        }
        else if (number % 5 == 0)
        {
            customer = "cliente-inestable";
        }

        return new Order(Guid.NewGuid(), number, customer, total);
    }
}
