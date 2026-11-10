# RabbitMQExample

[![CI](https://github.com/carlosalbertoxw/RabbitMQExample/actions/workflows/ci.yml/badge.svg)](https://github.com/carlosalbertoxw/RabbitMQExample/actions/workflows/ci.yml)

Seis ejemplos de mensajería con RabbitMQ en C# / .NET 10, ordenados de menor a mayor complejidad. Cada ejemplo tiene un proyecto de consola **Producer**, que envía mensajes, y otro **Consumer**, que los recibe. RabbitMQ corre en Docker con `docker compose`.

| # | Ejemplo | Qué aprendes |
|---|---------|--------------|
| 01 | [HelloWorld](01-HelloWorld/README.md) | Conectarse, declarar una cola, publicar y consumir. Lo mínimo indispensable. |
| 02 | [WorkQueues](02-WorkQueues/README.md) | Repartir tareas entre varios workers con confirmación manual (ack), `prefetch` y mensajes persistentes. |
| 03 | [PublishSubscribe](03-PublishSubscribe/README.md) | Enviar una copia de cada mensaje a todos los suscriptores con un exchange `fanout` y colas temporales. |
| 04 | [Topics](04-Topics/README.md) | Enrutar mensajes por patrones (`*.error`, `pagos.#`) con un exchange `topic`. |
| 05 | [Rpc](05-Rpc/README.md) | Petición y respuesta sobre RabbitMQ: `ReplyTo`, `CorrelationId`, timeouts y detección de "no hay servidor". |
| 06 | [ReliableMessaging](06-ReliableMessaging/README.md) | Un flujo pensado para no perder mensajes: confirmaciones del publicador, colas quorum, consumidor idempotente, reintentos con espera, cola de mensajes muertos, recuperación de la conexión y apagado ordenado con Generic Host. |

Cada carpeta tiene su propio README con el detalle, los comandos y experimentos para ver cada concepto en acción.

## Requisitos

- [.NET SDK 10](https://dotnet.microsoft.com/download). La versión mínima está fijada en [`global.json`](global.json).
- Docker con Docker Compose.

## 1. Levantar RabbitMQ

```bash
docker compose up -d --wait
```

Arranca un contenedor `rabbitmq-example` con RabbitMQ 4.4 y espera a que esté listo (`--wait` usa el *healthcheck* del compose).

| Puerto | Uso |
|--------|-----|
| `5672` | AMQP: aquí se conectan los Producer y Consumer |
| `15672` | Interfaz web de administración: http://localhost:15672 (usuario `guest`, contraseña `guest`) |

- Los puertos solo se publican en `127.0.0.1`, así que nadie más en tu red puede conectarse con las credenciales por defecto.
- Los datos se guardan en el volumen `rabbitmq-data`, así que colas y mensajes persistentes sobreviven a `docker compose down` / `up`.

Para detenerlo:

```bash
docker compose down
```

Para detenerlo y borrar también colas y mensajes (útil para empezar de cero):

```bash
docker compose down -v
```

## 2. Ejecutar un ejemplo

Cada ejemplo se ejecuta con dos terminales: una para el Consumer y otra para el Producer. Por ejemplo, el primero:

```bash
dotnet run --project 01-HelloWorld/Consumer
```

```bash
dotnet run --project 01-HelloWorld/Producer -- "Hola desde el Producer"
```

Los argumentos de cada ejemplo están en su README.

## Configuración

Todos los ejemplos se conectan a `localhost:5672` con `guest`/`guest`. Se puede cambiar con variables de entorno:

| Variable | Valor por defecto |
|----------|-------------------|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `guest` |
| `RABBITMQ_PASS` | `guest` |

Ejemplo en PowerShell:

```powershell
$env:RABBITMQ_HOST = "otro-servidor"; dotnet run --project 01-HelloWorld/Consumer
```

`docker-compose.yml` lee `RABBITMQ_USER` y `RABBITMQ_PASS` para crear el usuario del broker, desde el entorno o desde un archivo `.env` junto al compose (está en `.gitignore`). Esos valores solo se aplican la primera vez que el broker arranca con el volumen vacío; para cambiarlos después, usa `docker compose down -v`.

> `guest`/`guest` son las credenciales públicas por defecto de RabbitMQ y solo sirven para desarrollo local. Si reutilizas este código fuera de tu máquina, configura siempre las variables de entorno.

## Estructura del proyecto

```
RabbitMQExample/
├── docker-compose.yml          # RabbitMQ 4.4 con la interfaz de administración
├── RabbitMQExample.slnx        # Solución con los 12 proyectos, agrupados por ejemplo
├── global.json                 # Versión mínima del SDK de .NET
├── 01-HelloWorld/
│   ├── Producer/
│   └── Consumer/
├── 02-WorkQueues/              # misma estructura en cada ejemplo
├── 03-PublishSubscribe/
├── 04-Topics/
├── 05-Rpc/
├── 06-ReliableMessaging/
├── scripts/smoke-test.sh       # Prueba de humo de los 6 ejemplos
└── .github/                    # CI (GitHub Actions) y Dependabot
```

Los 12 proyectos son totalmente independientes: cada uno tiene su propio `.csproj` y no hay código compartido. Por eso partes como la conexión con reintentos se repiten en cada `Program.cs`: así cada archivo se puede leer y copiar por separado, sin saltar entre proyectos.

## Prueba de humo y CI

`scripts/smoke-test.sh` compila la solución, ejecuta los 6 ejemplos contra el RabbitMQ local y comprueba su salida:

```bash
docker compose up -d --wait
```

```bash
bash scripts/smoke-test.sh
```

En Windows se ejecuta con Git Bash. El workflow [`ci.yml`](.github/workflows/ci.yml) hace lo mismo en cada push y PR: levanta RabbitMQ con el mismo `docker-compose.yml`, verifica el formato (`dotnet format`), compila con las advertencias tratadas como errores, busca dependencias con vulnerabilidades conocidas y corre la prueba de humo. Dependabot propone cada semana las actualizaciones de los paquetes NuGet, de la imagen de RabbitMQ y de las actions.

## Licencia

[MIT](LICENSE)
