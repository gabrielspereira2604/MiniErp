using InvoiceService.Domain.Aggregates;

namespace InvoiceService.Domain.Ports;

public interface IInvoiceRepository
{
    Task<Invoice?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    Task SaveAsync(Invoice invoice, CancellationToken cancellationToken = default);
}
