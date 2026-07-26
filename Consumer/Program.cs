using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string QueueName = "hello";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest"
};

Console.WriteLine($"[Consumer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Misma declaración que en el Producer: idempotente, no importa quién arranca primero.
// durable: true es obligatorio en RabbitMQ 4.2+ (las colas transitorias no exclusivas están deprecadas).
await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[Consumer] Recibido: \"{message}\"");
    return Task.CompletedTask;
};

// autoAck: true -> RabbitMQ da el mensaje por procesado en cuanto lo entrega.
// Suficiente para un ejemplo; en producción normalmente se confirma manualmente.
await channel.BasicConsumeAsync(queue: QueueName, autoAck: true, consumer: consumer);

Console.WriteLine("[Consumer] Esperando mensajes. Presiona Ctrl+C para salir.");

var exit = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // evitamos que el proceso muera de golpe para cerrar la conexión limpiamente
    exit.TrySetResult();
};
await exit.Task;

Console.WriteLine("[Consumer] Fin.");

static async Task<IConnection> ConnectWithRetryAsync(ConnectionFactory factory, int attempts = 5)
{
    for (var i = 1; ; i++)
    {
        try
        {
            return await factory.CreateConnectionAsync();
        }
        catch (Exception ex) when (i < attempts)
        {
            Console.WriteLine($"[Consumer] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en 3 s...");
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
    }
}
