# 01 · HelloWorld

El ejemplo más simple posible: el **Producer** publica texto en la cola `hello` y el **Consumer** lo imprime.

```
Producer ──► [cola "hello"] ──► Consumer
```

## Ejecutar

Con RabbitMQ levantado (`docker compose up -d --wait` en la raíz del repositorio), en una terminal:

```bash
dotnet run --project 01-HelloWorld/Consumer
```

En otra terminal. El Producer tiene dos modos:

```bash
# Un solo mensaje: envía el texto y termina
dotnet run --project 01-HelloWorld/Producer -- "Hola desde el Producer"

# Interactivo: cada línea que escribas se envía como un mensaje (línea vacía o Ctrl+C para salir)
dotnet run --project 01-HelloWorld/Producer
```

Salida esperada en el Consumer:

```
[Consumer] Recibido: "Hola desde el Producer"
```

El orden de arranque no importa: si el Producer envía mensajes sin ningún Consumer conectado, quedan en la cola y se entregan cuando el Consumer arranque. En http://localhost:15672, pestaña *Queues*, se ven la cola `hello`, los mensajes pendientes y los consumidores conectados.

## Cómo funciona

1. Ambos programas crean una conexión AMQP con `ConnectionFactory` (paquete NuGet [`RabbitMQ.Client`](https://www.nuget.org/packages/RabbitMQ.Client) 7.x, API asíncrona). Si RabbitMQ todavía está arrancando, reintentan con espera exponencial (1, 2, 4, 8 y 16 s); si el usuario o la contraseña son incorrectos, fallan de inmediato porque esperar no lo arregla.
2. Ambos declaran la cola `hello` con `QueueDeclareAsync`. Declarar es idempotente, así que no importa cuál arranca primero. Es `durable: true` porque RabbitMQ 4.x ya no permite por defecto colas transitorias que no sean exclusivas.
3. El **Producer** publica con `BasicPublishAsync` en el *exchange* por defecto (`""`), que entrega el mensaje a la cola cuyo nombre coincide con la *routing key*.
4. El **Consumer** registra un `AsyncEventingBasicConsumer` con `autoAck: true`, y con Ctrl+C cierra la conexión limpiamente.

## Limitaciones a propósito

Este ejemplo prioriza la simplicidad, así que **puede perder mensajes**:

- Con `autoAck: true`, RabbitMQ da cada mensaje por procesado en cuanto lo entrega. Si el Consumer falla a medio camino, el mensaje se pierde.
- Los mensajes no se publican como persistentes. La cola sobrevive a un reinicio del broker, pero los mensajes que tenía no.

El [ejemplo 02](../02-WorkQueues/README.md) resuelve ambas cosas.
