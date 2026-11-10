namespace ReliableMessaging.Consumer;

// Contrato del mensaje. Llega como JSON (camelCase) y el Producer tiene su propia copia:
// los proyectos no comparten código, solo el formato del mensaje.
internal sealed record Order(Guid Id, int Number, string Customer, decimal Total);
