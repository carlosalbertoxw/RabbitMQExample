using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

// Uso: dotnet run -- <patrón> [<patrón> ...]      p. ej.: dotnet run -- "*.error" "pagos.#"
//   *  coincide con exactamente una palabra
//   #  coincide con cero o más palabras
// Sin argumentos se suscribe a "#" (todo).
const string ExchangeName = "events";

var bindingKeys = args.Length > 0 ? args : ["#"];
var name = $"Suscriptor[{string.Join(", ", bindingKeys)}]";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = $"04-topics-consumer-{Environment.ProcessId}"
};

Console.WriteLine($"[{name}] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory, name);
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Topic, durable: true, autoDelete: false);

// Cola temporal y exclusiva, como en el ejemplo 03, pero ahora con un enlace por cada patrón.
// Si un mensaje coincide con varios patrones, la cola lo recibe una sola vez.
var queue = await channel.QueueDeclareAsync();
foreach (var bindingKey in bindingKeys)
{
    await channel.QueueBindAsync(queue: queue.QueueName, exchange: ExchangeName, routingKey: bindingKey);
}

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($"[{name}] Recibido [{ea.RoutingKey}]: \"{Encoding.UTF8.GetString(ea.Body.Span)}\"");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);

Console.WriteLine($"[{name}] Escuchando. Presiona Ctrl+C para salir.");

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
