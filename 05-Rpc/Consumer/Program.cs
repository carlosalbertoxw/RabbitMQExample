using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

// Servidor RPC: atiende peticiones de la cola "rpc_factorize" y responde a la cola que indica cada petición.
const string QueueName = "rpc_factorize";
const long MaxNumber = 1_000_000_000_000; // con este límite la factorización tarda milisegundos

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = $"05-rpc-server-{Environment.ProcessId}"
};

Console.WriteLine($"[Servidor] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory);
await using var channel = await connection.CreateChannelAsync();

// La cola de peticiones la declara solo el servidor: si el servidor nunca arrancó, la cola no existe
// y el cliente se entera de inmediato (ver BasicReturnAsync en el Producer).
await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

// Una petición a la vez por servidor; si arrancas varios servidores, RabbitMQ reparte entre ellos.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var request = ea.BasicProperties;
    var input = Encoding.UTF8.GetString(ea.Body.Span).Trim();

    // Una entrada inválida no es una excepción: se responde con un error para que el cliente no espere en vano.
    var response = long.TryParse(input, out var number) && number is >= 2 and <= MaxNumber
        ? $"{number} = {string.Join(" × ", Factorize(number))}"
        : $"Error: \"{input}\" no es un entero entre 2 y {MaxNumber:N0}";

    Console.WriteLine($"[Servidor] Petición {request.CorrelationId}: \"{input}\" -> \"{response}\"");

    if (string.IsNullOrEmpty(request.ReplyTo))
    {
        // Sin ReplyTo no hay a quién responder; se confirma igual para que no se quede en la cola para siempre.
        Console.WriteLine("[Servidor] La petición no indica ReplyTo; se descarta.");
    }
    else
    {
        // El CorrelationId es lo que permite al cliente saber a qué petición corresponde esta respuesta.
        var reply = new BasicProperties { CorrelationId = request.CorrelationId };
        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: request.ReplyTo,
            mandatory: false,
            basicProperties: reply,
            body: Encoding.UTF8.GetBytes(response));
    }

    // Se confirma después de responder: si el servidor muere antes, otra instancia atiende la petición.
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer);

Console.WriteLine($"[Servidor] Esperando peticiones en \"{QueueName}\". Presiona Ctrl+C para salir.");

var exit = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    exit.TrySetResult();
};
await exit.Task;

Console.WriteLine("[Servidor] Fin.");

// Factorización por divisiones sucesivas: suficiente para números de hasta 10^12.
static List<long> Factorize(long number)
{
    var factors = new List<long>();
    for (long divisor = 2; divisor * divisor <= number; divisor++)
    {
        while (number % divisor == 0)
        {
            factors.Add(divisor);
            number /= divisor;
        }
    }

    if (number > 1)
    {
        factors.Add(number);
    }

    return factors;
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
            Console.WriteLine($"[Servidor] Intento {i}/{attempts} falló ({ex.Message}). Reintentando en {delay.TotalSeconds} s...");
            await Task.Delay(delay);
        }
    }
}
