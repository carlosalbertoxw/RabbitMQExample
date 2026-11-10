using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string QueueName = "task_queue";

// Identifica a cada worker en la salida cuando hay varios corriendo a la vez.
var worker = $"Worker-{Environment.ProcessId}";

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
    Port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest",
    ClientProvidedName = $"02-work-queues-consumer-{Environment.ProcessId}"
};

Console.WriteLine($"[{worker}] Conectando a RabbitMQ en {factory.HostName}:{factory.Port} ...");

await using var connection = await ConnectWithRetryAsync(factory, worker);
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

// prefetchCount: 1 -> RabbitMQ no le entrega a este worker una tarea nueva hasta que confirme la anterior.
// Sin esto, el broker reparte por turnos (round-robin) sin importar si un worker sigue ocupado.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

// Garantiza que solo haya una tarea en curso y permite esperar a que termine al apagar.
var busy = new SemaphoreSlim(1, 1);
var stopping = false;

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    await busy.WaitAsync();
    try
    {
        if (stopping)
        {
            return; // sin ack: al cerrar el canal RabbitMQ la devuelve a la cola para otro worker
        }

        var message = Encoding.UTF8.GetString(ea.Body.Span);
        // Redelivered indica que otro worker la recibió antes y murió sin confirmarla.
        var note = ea.Redelivered ? " (reentregada)" : string.Empty;
        Console.WriteLine($"[{worker}] Recibida: \"{message}\"{note}");

        var seconds = message.Count(c => c == '.');
        await Task.Delay(TimeSpan.FromSeconds(seconds));

        // Ack manual: solo ahora RabbitMQ borra la tarea. Si el proceso muere antes de esta línea,
        // la tarea vuelve a la cola y la recibe otro worker.
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        Console.WriteLine($"[{worker}] Terminada: \"{message}\"");
    }
    finally
    {
        busy.Release();
    }
};

// autoAck: false -> confirmamos cada tarea nosotros, después de terminarla.
var consumerTag = await channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer);

Console.WriteLine($"[{worker}] Esperando tareas. Ctrl+C termina la tarea en curso y sale; Ctrl+C dos veces sale de inmediato.");

var exit = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    // La segunda vez dejamos que el proceso muera: sirve para ver cómo RabbitMQ reentrega la tarea.
    e.Cancel = exit.TrySetResult();
};
await exit.Task;

// Apagado ordenado: dejar de recibir, terminar la tarea en curso y luego cerrar.
Console.WriteLine($"[{worker}] Apagando: esperando a que termine la tarea en curso...");
stopping = true;
await channel.BasicCancelAsync(consumerTag);
await busy.WaitAsync(); // espera a que el handler suelte el semáforo
busy.Release();         // y lo libera para que cualquier entrega pendiente vea "stopping" y salga

Console.WriteLine($"[{worker}] Fin.");

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
