using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// Uso: dotnet run -- <origen.severidad> "mensaje"      p. ej.: dotnet run -- pagos.error "Cobro rechazado"
//      dotnet run                                       modo interactivo: cada línea es "<origen.severidad> mensaje"
const string ExchangeName = "events";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = "04-topics-producer"
};

if (args.Length == 1)
{
    Console.WriteLine("Uso: dotnet run -- <origen.severidad> \"mensaje\"   (o sin argumentos para el modo interactivo)");
    return 1;
}

Console.WriteLine($"[Producer] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Un exchange "topic" entrega cada mensaje a las colas cuyo patrón de enlace coincide con la routing key.
// La routing key son palabras separadas por puntos; aquí usamos el formato <origen>.<severidad>.
await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Topic, durable: true, autoDelete: false);

Console.WriteLine("[Producer] Conectado.");

if (args.Length >= 2)
{
    await PublishAsync(args[0], string.Join(' ', args[1..]));
}
else
{
    Console.WriteLine("[Producer] Escribe \"<origen.severidad> mensaje\" (p. ej. \"pagos.error Cobro rechazado\") y presiona Enter.");
    Console.WriteLine("[Producer] Línea vacía o Ctrl+C para salir.");

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    while (await ReadLineAsync(cts.Token) is { Length: > 0 } line)
    {
        var parts = line.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length < 2)
        {
            Console.WriteLine("[Producer] Formato: <origen.severidad> mensaje");
            continue;
        }

        await PublishAsync(parts[0], parts[1]);
    }
}

Console.WriteLine("[Producer] Fin.");
return 0;

async Task PublishAsync(string routingKey, string message)
{
    // Validamos antes de publicar: RabbitMQ acepta casi cualquier cadena, pero una clave mal formada
    // simplemente no coincide con ningún enlace y el mensaje se pierde sin aviso.
    if (!IsValidRoutingKey(routingKey, out var error))
    {
        Console.WriteLine($"[Producer] Routing key \"{routingKey}\" no válida: {error}");
        return;
    }

    await channel.BasicPublishAsync(exchange: ExchangeName, routingKey: routingKey, body: Encoding.UTF8.GetBytes(message));
    Console.WriteLine($"[Producer] Publicado [{routingKey}]: \"{message}\"");
}

static bool IsValidRoutingKey(string key, out string error)
{
    error = key switch
    {
        _ when Encoding.UTF8.GetByteCount(key) > 255 => "no puede pasar de 255 bytes",
        _ when key.Contains('*') || key.Contains('#') => "'*' y '#' son comodines de los enlaces, no se usan al publicar",
        _ when key.Any(char.IsWhiteSpace) || key.Split('.').Any(word => word.Length == 0) =>
            "debe ser una lista de palabras separadas por puntos, sin espacios, como pagos.error",
        _ => string.Empty
    };
    return error.Length == 0;
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
