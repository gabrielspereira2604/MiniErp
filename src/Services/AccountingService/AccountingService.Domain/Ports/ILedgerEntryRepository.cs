using AccountingService.Domain.Aggregates;

namespace AccountingService.Domain.Ports;

public interface ILedgerEntryRepository
{
    Task<LedgerEntry?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    Task<LedgerEntry?> GetByReceivableIdAsync(Guid receivableId, CancellationToken cancellationToken = default);
    Task SaveAsync(LedgerEntry ledgerEntry, CancellationToken cancellationToken = default);
}
