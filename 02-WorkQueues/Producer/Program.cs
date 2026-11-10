using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// Uso: dotnet run -- [cantidad de tareas]   (10 por defecto)
const string QueueName = "task_queue";

var count = args.Length > 0 && int.TryParse(args[0], out var parsed) && parsed > 0 ? parsed : 10;

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = "02-work-queues-producer"
};

Console.WriteLine($"[Producer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Cola durable: su definición sobrevive a un reinicio del broker.
await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

// Persistent = true: RabbitMQ escribe el mensaje a disco, así que también sobrevive a un reinicio.
// Hace falta las dos cosas: una cola durable con mensajes no persistentes los pierde igual.
var properties = new BasicProperties { Persistent = true };

for (var i = 1; i <= count; i++)
{
    // Cada punto representa un segundo de "trabajo" que el Consumer simulará.
    var seconds = Random.Shared.Next(1, 5);
    var message = $"Tarea {i} {new string('.', seconds)}";

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: QueueName,
        mandatory: false,
        basicProperties: properties,
        body: Encoding.UTF8.GetBytes(message));

    Console.WriteLine($"[Producer] Enviada: \"{message}\"");
}

Console.WriteLine($"[Producer] {count} tareas encoladas. Fin.");

// RabbitMQ puede tardar unos segundos en estar listo tras "docker compose up",
// así que reintentamos con espera exponencial (1, 2, 4, 8 y 16 s) antes de rendirnos.
// Un usuario o contraseña incorrectos no se arreglan esperando, así que ese error no se reintenta.
static async Task<IConnection> ConnectWithRetryAsync(ConnectionFactory factory, int attempts = 6)
{
    for (var i = 1; ; i++)
    {
        try
        {
            return await factory.CreateConnectionAsync();
        }
        catch (BrokerUnreachableException ex) when (i < attempts && ex.InnerException is not AuthenticationFailureException)
        {
            var delay = TimeSpan.FromSeconds(Math.Pow(2, i - 1));
            Console.WriteLine($"[Producer] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en {delay.TotalSeconds} s...");
            await Task.Delay(delay);
        }
    }
}
