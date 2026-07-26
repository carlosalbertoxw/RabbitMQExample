using System.Text;
using RabbitMQ.Client;

const string QueueName = "hello";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest"
};

Console.WriteLine($"[Producer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Declarar la cola es idempotente: la crea si no existe, no hace nada si ya existe.
// Producer y Consumer la declaran para que no importe cuál arranca primero.
// durable: true es obligatorio en RabbitMQ 4.2+ (las colas transitorias no exclusivas están deprecadas).
await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

Console.WriteLine("[Producer] Conectado.");

if (args.Length > 0)
{
    // Modo un solo mensaje: dotnet run -- "mi mensaje"
    await SendAsync(string.Join(' ', args));
}
else
{
    // Modo interactivo: cada línea escrita se envía como un mensaje.
    Console.WriteLine("[Producer] Escribe un mensaje y presiona Enter para enviarlo.");
    Console.WriteLine("[Producer] Línea vacía o Ctrl+C para salir.");

    while (Console.ReadLine() is { Length: > 0 } line)
    {
        await SendAsync(line);
    }
}

Console.WriteLine("[Producer] Fin.");

async Task SendAsync(string message)
{
    var body = Encoding.UTF8.GetBytes(message);
    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName, body: body);
    Console.WriteLine($"[Producer] Enviado: \"{message}\"");
}

// RabbitMQ puede tardar unos segundos en estar listo tras "docker compose up",
// así que reintentamos la conexión unas cuantas veces antes de rendirnos.
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
            Console.WriteLine($"[Producer] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en 3 s...");
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
    }
}
