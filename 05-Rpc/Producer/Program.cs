using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

// Cliente RPC. Uso: dotnet run -- 360 97 1000000007   (o sin argumentos para el modo interactivo)
const string RequestQueue = "rpc_factorize";
var timeout = TimeSpan.FromSeconds(10);

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = "05-rpc-client"
};

Console.WriteLine($"[Cliente] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// Peticiones enviadas que todavía esperan respuesta, indexadas por su CorrelationId.
var pending = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

// Cola de respuestas: temporal, exclusiva de este cliente y con nombre generado por el broker.
var replyQueue = (await channel.QueueDeclareAsync()).QueueName;

var replyConsumer = new AsyncEventingBasicConsumer(channel);
replyConsumer.ReceivedAsync += (_, ea) =>
{
    // Una respuesta sin petición pendiente llegó tarde (la petición ya expiró) y se ignora.
    if (ea.BasicProperties.CorrelationId is { } id && pending.TryRemove(id, out var call))
    {
        call.TrySetResult(Encoding.UTF8.GetString(ea.Body.Span));
    }

    return Task.CompletedTask;
};
await channel.BasicConsumeAsync(queue: replyQueue, autoAck: true, consumer: replyConsumer);

// Con mandatory: true, si ninguna cola recibe la petición (el servidor nunca la declaró),
// RabbitMQ la devuelve en lugar de descartarla. Así fallamos de inmediato en vez de esperar el timeout.
channel.BasicReturnAsync += (_, ea) =>
{
    if (ea.BasicProperties.CorrelationId is { } id && pending.TryRemove(id, out var call))
    {
        call.TrySetException(new InvalidOperationException(
            $"no existe la cola \"{RequestQueue}\" ({ea.ReplyText}). ¿Arrancaste el servidor (Consumer)?"));
    }

    return Task.CompletedTask;
};

Console.WriteLine("[Cliente] Conectado.");

if (args.Length > 0)
{
    // Se envían todas las peticiones antes de esperar ninguna respuesta:
    // el CorrelationId permite emparejar cada respuesta aunque lleguen en otro orden.
    var calls = new List<(string Input, Task<string> Response)>();
    foreach (var input in args)
    {
        calls.Add((input, await SendRequestAsync(input)));
    }

    foreach (var (input, response) in calls)
    {
        await PrintResponseAsync(input, response);
    }
}
else
{
    Console.WriteLine("[Cliente] Escribe un número y presiona Enter para factorizarlo.");
    Console.WriteLine("[Cliente] Línea vacía o Ctrl+C para salir.");

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    while (await ReadLineAsync(cts.Token) is { Length: > 0 } line)
    {
        await PrintResponseAsync(line, await SendRequestAsync(line));
    }
}

Console.WriteLine("[Cliente] Fin.");

// Publica la petición y devuelve una tarea que se completa con la respuesta, con un error o al vencer el plazo.
async Task<Task<string>> SendRequestAsync(string input)
{
    var correlationId = Guid.NewGuid().ToString("N");
    var call = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    pending[correlationId] = call;

    var properties = new BasicProperties
    {
        CorrelationId = correlationId,
        ReplyTo = replyQueue,
        // Si nadie la atiende antes del plazo, RabbitMQ la descarta: a esas alturas nadie espera la respuesta.
        Expiration = ((long)timeout.TotalMilliseconds).ToString()
    };

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: RequestQueue,
        mandatory: true,
        basicProperties: properties,
        body: Encoding.UTF8.GetBytes(input));

    return WaitForReplyAsync(correlationId, call.Task);
}

async Task<string> WaitForReplyAsync(string correlationId, Task<string> reply)
{
    try
    {
        return await reply.WaitAsync(timeout);
    }
    finally
    {
        pending.TryRemove(correlationId, out _);
    }
}

async Task PrintResponseAsync(string input, Task<string> response)
{
    try
    {
        Console.WriteLine($"[Cliente] {await response}");
    }
    catch (TimeoutException)
    {
        Console.WriteLine($"[Cliente] \"{input}\": sin respuesta en {timeout.TotalSeconds} s. ¿El servidor está corriendo?");
    }
    catch (InvalidOperationException ex)
    {
        Console.WriteLine($"[Cliente] \"{input}\": {ex.Message}");
    }
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
            Console.WriteLine($"[Cliente] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en {delay.TotalSeconds} s...");
            await Task.Delay(delay);
        }
    }
}
