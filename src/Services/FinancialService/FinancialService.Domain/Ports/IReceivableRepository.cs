using FinancialService.Domain.Aggregates;

namespace FinancialService.Domain.Ports;

public interface IReceivableRepository
{
    Task<Receivable?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    Task<Receivable?> GetByInvoiceIdAsync(Guid invoiceId, CancellationToken cancellationToken = default);
    Task SaveAsync(Receivable receivable, CancellationToken cancellationToken = default);
}
