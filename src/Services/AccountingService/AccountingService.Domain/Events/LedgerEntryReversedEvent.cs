using Shared.Kernel;

namespace AccountingService.Domain.Events;

public record LedgerEntryReversedEvent(
    Guid LedgerEntryId,
    Guid ReceivableId,
    string Reason
) : DomainEvent;
