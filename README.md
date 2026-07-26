# RabbitMQExample

Ejemplo básico de mensajería con RabbitMQ en C# / .NET 10: un proyecto de consola **Producer** publica mensajes en una cola llamada `hello` y otro proyecto de consola **Consumer** los recibe y los imprime. RabbitMQ corre en Docker mediante `docker compose`.

## Estructura del proyecto

```
RabbitMQExample/
├── docker-compose.yml        # Levanta RabbitMQ (imagen rabbitmq:latest)
├── rabbitmq/
│   └── enabled_plugins       # Habilita la interfaz web de administración
├── Producer/                 # Consola que ENVÍA mensajes a la cola "hello"
│   └── Program.cs
├── Consumer/                 # Consola que RECIBE mensajes de la cola "hello"
│   └── Program.cs
└── RabbitMQExample.slnx      # Solución (los proyectos son independientes)
```

Los dos proyectos son totalmente independientes: cada uno tiene su propio `.csproj`, no se referencian entre sí y se pueden ejecutar por separado y en cualquier orden (ambos declaran la cola, que es una operación idempotente).

## Requisitos

- [.NET SDK 10](https://dotnet.microsoft.com/download) (probado con 10.0.301)
- Docker con Docker Compose (usa la imagen `rabbitmq:latest` ya descargada; no descarga nada nuevo si ya la tienes)

## 1. Levantar RabbitMQ

```bash
docker compose up -d
```

Esto arranca un contenedor llamado `rabbitmq-example` con:

| Puerto | Uso |
|--------|-----|
| `5672` | AMQP — aquí se conectan Producer y Consumer |
| `15672` | Interfaz web de administración |

- **Interfaz web:** http://localhost:15672 — usuario `guest`, contraseña `guest`. Ahí puedes ver la cola `hello`, cuántos mensajes tiene encolados y los consumidores conectados.
- El compose incluye un *healthcheck*; puedes comprobar el estado con `docker compose ps` (espera a ver `healthy`).

Para detenerlo:

```bash
docker compose down
```

## 2. Ejecutar el Consumer (recibe mensajes)

En una terminal:

```bash
dotnet run --project Consumer
```

Queda a la escucha de la cola `hello` e imprime cada mensaje que llega. Se cierra con `Ctrl+C`.

## 3. Ejecutar el Producer (envía mensajes)

En **otra** terminal. Tiene dos modos:

```bash
# Modo un solo mensaje: envía el texto pasado como argumento y termina
dotnet run --project Producer -- "Hola desde el Producer"

# Modo interactivo: cada línea que escribas se envía como un mensaje
# (línea vacía o Ctrl+C para salir)
dotnet run --project Producer
```

Salida esperada en el Consumer:

```
[Consumer] Recibido: "Hola desde el Producer"
```

El orden no importa: si el Producer envía mensajes sin que haya ningún Consumer conectado, los mensajes quedan encolados en RabbitMQ y se entregan cuando el Consumer arranque.

## Configuración

Por defecto ambos proyectos se conectan a `localhost:5672` con `guest`/`guest`. Se puede cambiar con variables de entorno (útil si RabbitMQ corre en otra máquina o con otras credenciales):

| Variable | Valor por defecto |
|----------|-------------------|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `guest` |
| `RABBITMQ_PASS` | `guest` |

Ejemplo en PowerShell:

```powershell
$env:RABBITMQ_HOST = "otro-servidor"; dotnet run --project Consumer
```

## Cómo funciona

1. Ambos programas crean una conexión AMQP con `ConnectionFactory` (paquete NuGet [`RabbitMQ.Client`](https://www.nuget.org/packages/RabbitMQ.Client) 7.x, API asíncrona) y reintentan hasta 5 veces por si RabbitMQ todavía está arrancando.
2. Ambos declaran la cola `hello` con `QueueDeclareAsync` (`durable: true`, obligatorio en RabbitMQ 4.2+, donde las colas transitorias no exclusivas están deprecadas).
3. El **Producer** publica con `BasicPublishAsync` en el *exchange* por defecto (`""`), usando el nombre de la cola como *routing key*.
4. El **Consumer** registra un `AsyncEventingBasicConsumer` con `BasicConsumeAsync` y `autoAck: true`: RabbitMQ da por procesado cada mensaje en cuanto lo entrega (suficiente para un ejemplo; en producción se suele confirmar manualmente con `BasicAckAsync`).

## Notas

- La imagen `rabbitmq:latest` no trae la interfaz web activada; el archivo `rabbitmq/enabled_plugins` (montado como volumen en el compose) habilita el plugin `rabbitmq_management` al arrancar.
- La cola es durable pero los mensajes no se publican como persistentes: si el contenedor se reinicia, los mensajes encolados se pierden. Para un ejemplo básico es suficiente.
