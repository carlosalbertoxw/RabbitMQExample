# 05 · Rpc

Petición y respuesta sobre RabbitMQ (*Remote Procedure Call*). El **Producer** hace de cliente: pide factorizar un número y espera la respuesta. El **Consumer** hace de servidor: calcula y responde.

```
Cliente (Producer) ──► [rpc_factorize] ──► Servidor (Consumer)
      ▲                                          │
      └──────── [amq.gen-... respuestas] ◄───────┘
         ReplyTo = cola de respuestas, CorrelationId = id de la petición
```

## Ejecutar

Arranca uno o varios servidores:

```bash
dotnet run --project 05-Rpc/Consumer
```

Y envía peticiones (cada argumento es una petición; sin argumentos entra en modo interactivo):

```bash
dotnet run --project 05-Rpc/Producer -- 360 97 1000000007 hola
```

```
[Cliente] 360 = 2 × 2 × 2 × 3 × 3 × 5
[Cliente] 97 = 97
[Cliente] 1000000007 = 1000000007
[Cliente] Error: "hola" no es un entero entre 2 y 1,000,000,000,000
```

## Cómo funciona

1. El cliente crea una **cola de respuestas** temporal y exclusiva, y la consume.
2. Cada petición lleva dos propiedades: **`ReplyTo`**, el nombre de la cola de respuestas, y **`CorrelationId`**, un id único.
3. El servidor procesa la petición, publica la respuesta en la cola `ReplyTo` con el mismo `CorrelationId` y solo entonces hace ack de la petición.
4. El cliente guarda las peticiones pendientes en un diccionario por `CorrelationId` y completa cada una cuando llega su respuesta. Como todas las peticiones se envían antes de esperar ninguna respuesta, el emparejamiento funciona aunque las respuestas lleguen en otro orden.

## Robustez

| Situación | Qué hace el ejemplo |
|-----------|---------------------|
| El servidor nunca arrancó y la cola `rpc_factorize` no existe | La petición se publica con `mandatory: true`, así que RabbitMQ la devuelve (`BasicReturnAsync`) y el cliente falla **de inmediato** en lugar de esperar. Por eso solo el servidor declara la cola. |
| La cola existe, pero ningún servidor la atiende | El cliente se rinde a los **10 s** (`WaitAsync(timeout)`). La petición lleva `Expiration` = 10 s, así que RabbitMQ la descarta: nadie va a usar esa respuesta. |
| Entrada inválida | El servidor responde con un mensaje de error en lugar de lanzar una excepción; así el cliente no espera en vano. |
| Respuesta tardía de una petición que ya expiró | El cliente la ignora: ya no está en el diccionario de pendientes. |
| Varios servidores | Con `prefetchCount: 1`, RabbitMQ reparte las peticiones entre ellos. |

## Experimentos

1. **Sin servidor.** Borra la cola `rpc_factorize` desde la interfaz web (*Queues → rpc_factorize → Delete*) y ejecuta el cliente: falla al instante con `NO_ROUTE`.
2. **Servidor detenido.** Arranca y detén el servidor (la cola queda creada) y ejecuta el cliente: espera 10 s y reporta que no hubo respuesta.
3. **Varios servidores.** Arranca dos servidores y envía muchas peticiones: el log de cada servidor muestra cuáles atendió.

## Cuándo usar RPC sobre mensajería

Acopla al cliente con la disponibilidad del servidor, igual que una llamada HTTP, así que úsalo con cuidado. Si el cliente no necesita la respuesta para continuar, publicar un evento (ejemplos 03 y 04) suele ser mejor.

El [ejemplo 06](../06-ReliableMessaging/README.md) junta todo lo anterior en un flujo diseñado para no perder mensajes.
