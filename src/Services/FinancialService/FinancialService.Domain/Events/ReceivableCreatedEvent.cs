using Shared.Kernel;

namespace FinancialService.Domain.Events;

public record ReceivableCreatedEvent(
    Guid ReceivableId,
    Guid InvoiceId,
    decimal Amount,
    string Currency
) : DomainEvent;
