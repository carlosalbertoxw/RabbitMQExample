using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

// Uso: dotnet run -- [nombre del suscriptor]
const string ExchangeName = "notifications";

var name = args.Length > 0 ? args[0] : $"Suscriptor-{Environment.ProcessId}";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = $"03-publish-subscribe-consumer-{name}"
};

Console.WriteLine($"[{name}] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory, name);
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Fanout, durable: true, autoDelete: false);

// Cada suscriptor necesita su propia cola para recibir su propia copia de cada mensaje.
// Sin argumentos, QueueDeclareAsync crea una cola con nombre aleatorio (amq.gen-...), exclusiva
// de esta conexión y que se borra sola al desconectarse: justo lo que necesita un suscriptor temporal.
var queue = await channel.QueueDeclareAsync();
await channel.QueueBindAsync(queue: queue.QueueName, exchange: ExchangeName, routingKey: string.Empty);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($"[{name}] Recibido: \"{Encoding.UTF8.GetString(ea.Body.Span)}\"");
    return Task.CompletedTask;
};

// autoAck: true es razonable aquí: son notificaciones informativas y perder una no es grave.
await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"[{name}] Suscrito con la cola \"{queue.QueueName}\". Presiona Ctrl+C para salir.");

var exit = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    exit.TrySetResult();
};
await exit.Task;

Console.WriteLine($"[{name}] Fin.");

// RabbitMQ puede tardar unos segundos en estar listo tras "docker compose up",
// así que reintentamos con espera exponencial (1, 2, 4, 8 y 16 s) antes de rendirnos.
// Un usuario o contraseña incorrectos no se arreglan esperando, así que ese error no se reintenta.
static async Task<IConnection> ConnectWithRetryAsync(ConnectionFactory factory, string name, int attempts = 6)
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
            Console.WriteLine($"[{name}] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en {delay.TotalSeconds} s...");
            await Task.Delay(delay);
        }
    }
}
