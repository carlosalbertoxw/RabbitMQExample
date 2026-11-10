# 04 · Topics

Cada suscriptor elige qué mensajes recibe mediante patrones sobre la routing key. Los mensajes se publican con una clave `<origen>.<severidad>`, como `pagos.error` o `inventario.info`.

```
                                         ┌─ "*.error" ──► [cola] ──► Alertas
Producer ──► exchange "events" (topic) ──┤
   pagos.error                           └─ "pagos.#" ──► [cola] ──► Equipo de pagos
```

## Ejecutar

Arranca suscriptores con distintos patrones (sin argumentos se suscribe a `#`, es decir, a todo):

```bash
dotnet run --project 04-Topics/Consumer -- "*.error"
```

```bash
dotnet run --project 04-Topics/Consumer -- "pagos.#"
```

Y publica con distintas claves:

```bash
dotnet run --project 04-Topics/Producer -- pagos.error "Cobro rechazado"
```

```bash
dotnet run --project 04-Topics/Producer -- inventario.info "Stock actualizado"
```

En modo interactivo (sin argumentos), cada línea se escribe como `pagos.error Cobro rechazado`.

## Comodines

| Comodín | Coincide con | Ejemplo |
|---------|--------------|---------|
| `*` | exactamente una palabra | `*.error` recibe `pagos.error`, pero no `pagos.tarjeta.error` |
| `#` | cero o más palabras | `pagos.#` recibe `pagos`, `pagos.error` y `pagos.tarjeta.error` |

Con los dos suscriptores de arriba:

| Routing key | `*.error` | `pagos.#` |
|-------------|:---------:|:---------:|
| `pagos.error` | ✅ | ✅ |
| `pagos.info` | | ✅ |
| `inventario.error` | ✅ | |
| `inventario.info` | | |

## Detalles que vale la pena notar

- Un suscriptor puede tener **varios patrones** (`dotnet run --project 04-Topics/Consumer -- "*.error" "pagos.#"`). Si un mensaje coincide con más de uno, la cola lo recibe una sola vez.
- El Producer **valida la routing key** antes de publicar: rechaza claves vacías, con espacios, con comodines o de más de 255 bytes. RabbitMQ las aceptaría, pero una clave mal formada no coincide con ningún enlace y el mensaje se pierde sin aviso.
- `topic` es el tipo de exchange más flexible: un binding `#` se comporta como `fanout`, y uno sin comodines se comporta como un exchange `direct`.

## Lo que todavía falta

Hasta aquí la comunicación va en un solo sentido. Para pedir algo y esperar la respuesta, sigue con el [ejemplo 05](../05-Rpc/README.md).
