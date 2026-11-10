using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace ReliableMessaging.Consumer;

internal static class RabbitMqConnection
{
    // Las mismas variables de entorno que el resto de los ejemplos (RABBITMQ_HOST, RABBITMQ_PORT, ...).
    // Con Generic Host también se pueden pasar como argumentos: dotnet run -- --RABBITMQ_HOST otro-servidor
    public static ConnectionFactory CreateFactory(IConfiguration configuration, string clientName) => new()
    {
        HostName = configuration["RABBITMQ_HOST"] ?? "localhost",
        Port = int.TryParse(configuration["RABBITMQ_PORT"], out var port) ? port : 5672,
        UserName = configuration["RABBITMQ_USER"] ?? "guest",
        Password = configuration["RABBITMQ_PASS"] ?? "guest",
        ClientProvidedName = clientName
        // La recuperación automática no necesita configuración: viene activada por defecto. Si la conexión
        // se cae después de establecerse, el cliente reconecta solo cada 5 s y vuelve a declarar colas,
        // enlaces y consumidores.
    };

    // La recuperación automática solo actúa sobre conexiones ya abiertas; la primera conexión se reintenta aquí.
    public static async Task<IConnection> ConnectAsync(
        ConnectionFactory factory, ILogger logger, CancellationToken cancellationToken, int attempts = 6)
    {
        logger.LogInformation("Conectando a RabbitMQ en {Host}:{Port}", factory.HostName, factory.Port);

        for (var i = 1; ; i++)
        {
            try
            {
                var connection = await factory.CreateConnectionAsync(cancellationToken);
                SubscribeToEvents(connection, logger);
                return connection;
            }
            catch (BrokerUnreachableException ex) when (i < attempts && ex.InnerException is not AuthenticationFailureException)
            {
                // Espera exponencial con jitter: si muchos clientes arrancan a la vez, no reintentan sincronizados.
                var delay = TimeSpan.FromSeconds(Math.Pow(2, i - 1)) + TimeSpan.FromMilliseconds(Random.Shared.Next(500));
                logger.LogWarning("Intento {Attempt}/{Attempts} de conexión falló ({Error}). Reintentando en {Delay:0.0} s",
                    i, attempts, ex.Message, delay.TotalSeconds);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private static void SubscribeToEvents(IConnection connection, ILogger logger)
    {
        connection.ConnectionShutdownAsync += (_, e) =>
        {
            if (e.Initiator != ShutdownInitiator.Application)
            {
                logger.LogWarning("Se perdió la conexión con RabbitMQ: {Reason}. Intentando recuperarla...", e.ReplyText);
            }

            return Task.CompletedTask;
        };
        connection.RecoverySucceededAsync += (_, _) =>
        {
            logger.LogInformation("Conexión con RabbitMQ recuperada");
            return Task.CompletedTask;
        };
        // El broker bloquea a los publicadores cuando se queda sin memoria o disco.
        connection.ConnectionBlockedAsync += (_, e) =>
        {
            logger.LogWarning("RabbitMQ bloqueó la publicación: {Reason}", e.Reason);
            return Task.CompletedTask;
        };
        connection.ConnectionUnblockedAsync += (_, _) =>
        {
            logger.LogInformation("RabbitMQ volvió a aceptar publicaciones");
            return Task.CompletedTask;
        };
    }
}
