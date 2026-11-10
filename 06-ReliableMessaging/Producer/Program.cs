using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ReliableMessaging.Producer;

// Uso: dotnet run -- [--count 20] [--interval 500]
//   --count     pedidos a publicar; 0 (por defecto) publica sin parar hasta Ctrl+C
//   --interval  milisegundos entre pedidos (1000 por defecto)
//
// Generic Host aporta configuración (argumentos y variables de entorno), inyección de dependencias,
// logging y apagado ordenado con Ctrl+C o SIGTERM, como en un servicio real.
var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
});

builder.Services.AddSingleton(RabbitMqConnection.CreateFactory(builder.Configuration, "06-reliable-producer"));
builder.Services.AddHostedService<OrderPublisher>();

await builder.Build().RunAsync();
