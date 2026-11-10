using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string QueueName = "hello";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    // Nombre con el que aparece la conexión en la pestaña "Connections" de la interfaz web.
    ClientProvidedName = "01-hello-world-consumer"
};

Console.WriteLine($"[Consumer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Misma declaración que en el Producer: idempotente, no importa quién arranca primero.
// durable: true porque RabbitMQ 4.x ya no permite por defecto colas transitorias no exclusivas.
await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.Span);
    Console.WriteLine($"[Consumer] Recibido: \"{message}\"");
    return Task.CompletedTask;
};

// autoAck: true -> RabbitMQ da el mensaje por procesado en cuanto lo entrega.
// Es lo más simple, pero si el proceso falla a medio camino el mensaje se pierde.
// El ejemplo 02-WorkQueues muestra la confirmación manual (ack), que lo evita.
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
            Console.WriteLine($"[Consumer] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en {delay.TotalSeconds} s...");
            await Task.Delay(delay);
        }
    }
}
