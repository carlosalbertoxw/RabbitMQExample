# 06 · ReliableMessaging

Un flujo de pedidos diseñado para **no perder mensajes ni procesarlos dos veces**, con la estructura de un servicio real. Junta lo de los ejemplos anteriores y agrega lo que les faltaba.

```
Producer ──► exchange "orders" ──(order.created)──► cola "orders" (quorum) ──► Consumer
                  ▲                                      │
                  │ al vencer el TTL (5 s)               │ nack / límite de entregas
                  │                                      ▼
            cola "orders.retry" ◄── error pasajero ── cola "orders.dead"
```

## Ejecutar

```bash
dotnet run --project 06-ReliableMessaging/Consumer
```

```bash
# 20 pedidos, uno cada 200 ms
dotnet run --project 06-ReliableMessaging/Producer -- --count 20 --interval 200

# Sin --count publica sin parar (uno por segundo) hasta Ctrl+C
dotnet run --project 06-ReliableMessaging/Producer
```

Algunos pedidos traen casos especiales a propósito para recorrer cada camino:

| Pedido | Qué pasa | Dónde termina |
|--------|----------|---------------|
| La mayoría | Se cobra a la primera | Procesado ✅ |
| Múltiplos de 5 (`cliente-inestable`) | El cobro falla 2 veces y funciona en el tercer intento | Procesado tras 2 reintentos ✅ |
| Múltiplos de 11 (`cliente-sin-servicio`) | El cobro falla siempre | `orders.dead`, tras 3 reintentos |
| Múltiplos de 7 (total negativo) | No pasa la validación; reintentar no lo arregla | `orders.dead`, de inmediato |

## Qué resuelve cada pieza

### Publicar sin perder mensajes (Producer)

- **Confirmaciones del publicador** (`CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)`): `BasicPublishAsync` no termina hasta que el broker confirma que guardó el mensaje, y lanza una excepción si lo rechaza.
- **`mandatory: true`**: si ninguna cola recibe el mensaje, el broker lo devuelve (`PublishReturnException`) en lugar de descartarlo en silencio.
- **Mensaje persistente + cola quorum**: el mensaje confirmado sobrevive a un reinicio del broker. Las colas quorum son la opción recomendada en RabbitMQ 4.x para datos que no se pueden perder.
- **Reintentos de publicación** con espera exponencial (hasta 31 s) si se cae la conexión, el tiempo que tarda la recuperación automática en reconectar.
- **`MessageId` estable**: se repite en cada reintento, para que el Consumer pueda detectar duplicados.

### Procesar sin perder ni duplicar (Consumer)

- **Ack manual después de procesar** y `prefetchCount: 10`.
- **Idempotencia**: RabbitMQ garantiza entrega *al menos una vez*, así que un mensaje puede llegar repetido (por ejemplo, si se cae la conexión justo después de procesarlo y antes del ack). El Consumer recuerda los `MessageId` ya procesados y confirma los duplicados sin volver a cobrar. Aquí se guardan en memoria; en producción irían en la misma base de datos que el resultado del procesamiento.
- **Reintentos diferidos**: ante un error pasajero, el Consumer publica una copia en `orders.retry` con la cabecera `x-retry-count` y hace ack del original. Esa cola no tiene consumidores: el mensaje espera 5 s (`x-message-ttl`) y RabbitMQ lo devuelve a `orders` (`x-dead-letter-exchange`). A diferencia de reencolar de inmediato, esto da tiempo a que el servicio externo se recupere.
- **Cola de mensajes muertos**: los mensajes inválidos o que agotaron sus reintentos se rechazan con `BasicNackAsync(requeue: false)` y la cola los envía a `orders.dead`, donde quedan para revisarlos. RabbitMQ les agrega la cabecera `x-death` con el motivo.
- **Límite de entregas** (`x-delivery-limit: 5`): si un mensaje hace caer al Consumer una y otra vez, la cola quorum deja de reentregarlo y lo manda a `orders.dead` en lugar de bloquear el procesamiento para siempre.

### Operar como un servicio (ambos)

- **Generic Host**: configuración por argumentos y variables de entorno, inyección de dependencias, logging con hora y apagado ordenado con Ctrl+C o `SIGTERM` (por ejemplo, `docker stop`).
- **Apagado ordenado del Consumer**: deja de recibir (`BasicCancelAsync`), termina el pedido en curso y cierra. Los que recibió y no llegó a procesar vuelven a la cola.
- **Recuperación automática**: si se cae la conexión, el cliente reconecta cada 5 s y vuelve a declarar la topología y los consumidores. Los eventos de conexión perdida, recuperada y bloqueada (broker sin memoria o disco) quedan en el log.
- **Logs con contexto**: cada línea de log del Consumer incluye el `MessageId` del mensaje que se procesa.

## Experimentos

1. **Recorrer todos los caminos.** Publica 15 pedidos con el Consumer corriendo y sigue el log: procesados, reintentos cada 5 s y mensajes enviados a `orders.dead`.
2. **Inspeccionar los mensajes muertos.** En http://localhost:15672, *Queues → orders.dead → Get messages*: verás el JSON del pedido y la cabecera `x-death`, con el motivo (`rejected`) y la cola de origen.
3. **Reiniciar el broker con todo corriendo.** Con el Producer publicando sin parar y el Consumer activo, ejecuta `docker compose restart rabbitmq`. Ambos registran que perdieron la conexión y, unos segundos después, que la recuperaron. El Producer reintenta el pedido que no alcanzó a confirmar y ninguno se pierde. Puede aparecer un `Pedido #N duplicado`: se procesó justo antes de la caída, el ack se perdió y RabbitMQ lo reentregó; la idempotencia evita cobrarlo dos veces.
4. **Apagado ordenado.** Presiona Ctrl+C en el Consumer mientras procesa: termina el pedido en curso antes de salir.
5. **Varios Consumers.** Arranca dos y comprueba que se reparten los pedidos.

## Si cambias la topología

Si modificas algún argumento de las colas en `Topology.cs` (TTL, límite de entregas, etc.), RabbitMQ rechazará la declaración con `PRECONDITION_FAILED` porque la cola ya existe con otros argumentos. Bórrala desde la interfaz web o reinicia todo con `docker compose down -v`.

## Qué faltaría para producción

- Guardar los `MessageId` procesados en una base de datos, en la misma transacción que el resultado, en lugar de en memoria.
- Si el pedido nace de una escritura en base de datos, usar el patrón *outbox* para que guardar y publicar sean atómicos.
- Métricas y alertas (por ejemplo, con OpenTelemetry) sobre el tamaño de `orders.dead` y la latencia de procesamiento.
- Credenciales propias con permisos mínimos y TLS en la conexión AMQP.
