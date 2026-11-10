# 02 · WorkQueues

Una cola de trabajo repartida entre varios **workers** (Consumers). Es el patrón para distribuir tareas lentas: cada tarea la procesa un solo worker, y si un worker muere a medio trabajo, la tarea no se pierde.

```
                          ┌──► Worker A
Producer ──► [task_queue] ┤
                          └──► Worker B
```

## Ejecutar

Abre dos o más terminales con un worker cada una:

```bash
dotnet run --project 02-WorkQueues/Consumer
```

Y en otra terminal encola tareas (10 por defecto):

```bash
dotnet run --project 02-WorkQueues/Producer -- 10
```

Cada tarea lleva entre 1 y 4 puntos (`Tarea 3 ...`); el worker simula un segundo de trabajo por cada punto.

## Qué hay de nuevo respecto al ejemplo 01

| Concepto | Dónde | Para qué |
|----------|-------|----------|
| Mensajes persistentes | `BasicProperties { Persistent = true }` en el Producer | La tarea sobrevive a un reinicio del broker. Hace falta cola durable **y** mensaje persistente. |
| Ack manual | `autoAck: false` + `BasicAckAsync` al terminar | RabbitMQ solo borra la tarea cuando el worker confirma que la terminó. |
| Prefetch | `BasicQosAsync(prefetchCount: 1)` | Un worker no recibe otra tarea hasta terminar la actual. Sin esto, RabbitMQ reparte por turnos aunque un worker siga ocupado con una tarea larga. |
| Apagado ordenado | Ctrl+C en el worker | Deja de recibir tareas, termina la que tiene en curso y sale. |

## Experimentos

1. **Reparto entre workers.** Con dos workers abiertos, encola 10 tareas: verás que se las reparten y que el que termina primero recibe la siguiente.
2. **Un worker muere a medio trabajo.** Encola tareas largas y, mientras un worker procesa una, presiona **Ctrl+C dos veces** en él (la primera pide un apagado ordenado; la segunda mata el proceso). El otro worker recibe esa tarea marcada como `(reentregada)`.
3. **Reinicio del broker.** Sin workers corriendo, encola tareas y ejecuta `docker compose restart rabbitmq`. Cuando vuelva, las tareas siguen en la cola, porque son persistentes.

## Lo que todavía falta

- El Producer no sabe si el broker recibió de verdad cada tarea: publica y sigue. Las *confirmaciones del publicador* lo resuelven en el [ejemplo 06](../06-ReliableMessaging/README.md).
- Aquí ninguna tarea falla. En un caso real hay que decidir qué hacer con una tarea que falla: reintentarla más tarde, cuántas veces y dónde dejarla si nunca funciona. El ejemplo 06 lo resuelve con reintentos diferidos y una cola de mensajes muertos.
