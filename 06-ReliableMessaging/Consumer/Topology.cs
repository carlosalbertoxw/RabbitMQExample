using RabbitMQ.Client;

namespace ReliableMessaging.Consumer;

// Exchanges y colas del ejemplo. Producer y Consumer declaran exactamente lo mismo, así que no importa
// cuál arranca primero. Si cambias algún argumento de una cola ya creada, RabbitMQ rechaza la declaración
// (PRECONDITION_FAILED): bórrala desde la interfaz web o con "docker compose down -v" y vuelve a arrancar.
//
//   Producer ──► exchange "orders" ──(order.created)──► cola "orders" ──► Consumer
//                     ▲                                    │  nack / límite de entregas
//                     │ al vencer el TTL                   ▼
//               cola "orders.retry" ◄── reintento ──   cola "orders.dead"
internal static class Topology
{
    public const string Exchange = "orders";
    public const string RoutingKey = "order.created";
    public const string Queue = "orders";
    public const string RetryQueue = "orders.retry";
    public const string DeadLetterQueue = "orders.dead";

    public static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    // Si un mensaje se reentrega más veces que esto (p. ej. porque hace caer al Consumer cada vez),
    // la cola quorum deja de reintentarlo y lo manda a la cola de mensajes muertos.
    public const int DeliveryLimit = 5;

    public static async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Direct, durable: true, autoDelete: false,
            cancellationToken: cancellationToken);

        // Colas quorum: replicadas y pensadas para datos que no se pueden perder. Son la opción recomendada
        // en RabbitMQ 4.x para colas durables (en un solo nodo funcionan igual, sin réplicas).
        await channel.QueueDeclareAsync(DeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(Queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                // Un nack sin reencolar, o superar el límite de entregas, envía el mensaje a "orders.dead".
                ["x-dead-letter-exchange"] = string.Empty,
                ["x-dead-letter-routing-key"] = DeadLetterQueue,
                ["x-delivery-limit"] = DeliveryLimit
            },
            cancellationToken: cancellationToken);
        await channel.QueueBindAsync(Queue, Exchange, RoutingKey, cancellationToken: cancellationToken);

        // Cola de espera para los reintentos: nadie la consume. Cada mensaje espera RetryDelay y,
        // al expirar, RabbitMQ lo reenvía al exchange principal como si fuera nuevo.
        await channel.QueueDeclareAsync(RetryQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-message-ttl"] = (int)RetryDelay.TotalMilliseconds,
                ["x-dead-letter-exchange"] = Exchange,
                ["x-dead-letter-routing-key"] = RoutingKey
            },
            cancellationToken: cancellationToken);
    }
}
