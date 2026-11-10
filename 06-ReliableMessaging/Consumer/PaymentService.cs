namespace ReliableMessaging.Consumer;

// Error pasajero: vale la pena reintentar más tarde (timeout, servicio caído, 503...).
internal sealed class TransientPaymentException(string message) : Exception(message);

// Simula una pasarela de pagos externa. Falla a propósito con ciertos clientes para mostrar los reintentos.
internal sealed class PaymentService
{
    public async Task ChargeAsync(Order order, int retry)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(200)); // latencia de la llamada externa

        switch (order.Customer)
        {
            case "cliente-sin-servicio":
                throw new TransientPaymentException("la pasarela de pagos no responde");
            case "cliente-inestable" when retry < 2:
                throw new TransientPaymentException("la pasarela de pagos devolvió 503");
        }
    }
}
