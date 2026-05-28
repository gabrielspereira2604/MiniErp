using Shared.Kernel;

namespace AccountingService.Domain.Events;

public record LedgerEntryCreatedEvent(
    Guid LedgerEntryId,
    Guid ReceivableId,
    decimal Amount,
    string Currency,
    string DebitAccount,
    string CreditAccount
) : DomainEvent;
