namespace ReliableMessaging.Producer;

// Contrato del mensaje. Se serializa como JSON (camelCase) y el Consumer tiene su propia copia:
// los proyectos no comparten código, solo el formato del mensaje.
internal sealed record Order(Guid Id, int Number, string Customer, decimal Total);
