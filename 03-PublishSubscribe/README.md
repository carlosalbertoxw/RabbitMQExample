# 03 · PublishSubscribe

Cada mensaje llega a **todos** los suscriptores conectados, no solo a uno. Sirve para notificaciones, invalidación de cachés o eventos que interesan a varios sistemas a la vez.

```
                                         ┌──► [amq.gen-A] ──► Suscriptor A
Producer ──► exchange "notifications" ───┤
              (fanout)                   └──► [amq.gen-B] ──► Suscriptor B
```

## Ejecutar

Abre dos o más suscriptores; el argumento es solo un nombre para distinguirlos:

```bash
dotnet run --project 03-PublishSubscribe/Consumer -- A
```

```bash
dotnet run --project 03-PublishSubscribe/Consumer -- B
```

Y publica desde otra terminal (con un argumento o en modo interactivo):

```bash
dotnet run --project 03-PublishSubscribe/Producer -- "Mantenimiento programado a las 22:00"
```

Ambos suscriptores imprimen el mismo mensaje.

## Qué hay de nuevo

| Concepto | Para qué |
|----------|----------|
| **Exchange** | El Producer ya no publica en una cola, sino en un exchange, y no sabe quién recibirá el mensaje. El exchange decide a qué colas copiarlo. |
| **Tipo `fanout`** | Copia cada mensaje a todas las colas enlazadas e ignora la routing key. |
| **Cola temporal** | `QueueDeclareAsync()` sin argumentos crea una cola con nombre aleatorio (`amq.gen-...`), exclusiva de la conexión, que se borra sola al desconectarse. Cada suscriptor tiene la suya y por eso recibe su propia copia. |
| **Binding** | `QueueBindAsync` enlaza la cola del suscriptor con el exchange. |

## Experimentos

1. **Nadie escuchando.** Publica un mensaje sin suscriptores y luego arranca uno: no lo recibe. Sin colas enlazadas, el exchange descarta el mensaje. En publicar/suscribir solo recibe quien está escuchando en ese momento.
2. **Colas que desaparecen.** Con dos suscriptores abiertos, mira la pestaña *Queues* de http://localhost:15672 y verás dos colas `amq.gen-...`. Cierra un suscriptor con Ctrl+C y su cola desaparece.
3. **Enlaces del exchange.** En *Exchanges → notifications* se ven los *bindings* activos, uno por suscriptor.

## Lo que todavía falta

Todos reciben todo. Para que cada suscriptor elija qué mensajes le interesan, sigue con el [ejemplo 04](../04-Topics/README.md).
