using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ReliableMessaging.Consumer;

// Uso: dotnet run
//
// Generic Host aporta configuración (argumentos y variables de entorno), inyección de dependencias,
// logging y apagado ordenado con Ctrl+C o SIGTERM, como en un servicio real.
var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
    options.IncludeScopes = true; // muestra el MessageId en cada línea de log del mensaje que se procesa
});

builder.Services.AddSingleton(RabbitMqConnection.CreateFactory(builder.Configuration, "06-reliable-consumer"));
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddHostedService<OrderProcessor>();

await builder.Build().RunAsync();
