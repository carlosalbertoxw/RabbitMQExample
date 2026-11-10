using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// Uso: dotnet run -- "mensaje"   o sin argumentos para el modo interactivo.
const string ExchangeName = "notifications";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = "03-publish-subscribe-producer"
};

Console.WriteLine($"[Producer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Un exchange "fanout" copia cada mensaje a todas las colas enlazadas a él e ignora la routing key.
// El Producer ya no conoce ninguna cola: solo publica en el exchange.
await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Fanout, durable: true, autoDelete: false);

Console.WriteLine("[Producer] Conectado.");

if (args.Length > 0)
{
    await PublishAsync(string.Join(' ', args));
}
else
{
    Console.WriteLine("[Producer] Escribe una notificación y presiona Enter para publicarla.");
    Console.WriteLine("[Producer] Línea vacía o Ctrl+C para salir.");

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    while (await ReadLineAsync(cts.Token) is { Length: > 0 } line)
    {
        await PublishAsync(line);
    }
}

Console.WriteLine("[Producer] Fin.");

async Task PublishAsync(string message)
{
    var text = $"[{DateTime.Now:HH:mm:ss}] {message}";
    // Si en este momento no hay ningún suscriptor, ninguna cola está enlazada y el mensaje se descarta:
    // en publicar/suscribir solo reciben quienes están escuchando en ese momento.
    await channel.BasicPublishAsync(exchange: ExchangeName, routingKey: string.Empty, body: Encoding.UTF8.GetBytes(text));
    Console.WriteLine($"[Producer] Publicado: \"{text}\"");
}

// Console.ReadLine no acepta un CancellationToken; WaitAsync deja de esperarla al cancelar.
static async Task<string?> ReadLineAsync(CancellationToken cancellationToken)
{
    try
    {
        return await Task.Run(Console.ReadLine, CancellationToken.None).WaitAsync(cancellationToken);
    }
    catch (OperationCanceledException)
    {
        return null;
    }
}

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
