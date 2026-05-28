using Shared.Kernel;

namespace FinancialService.Domain.Events;

public record ReceivableReversedEvent(Guid ReceivableId, Guid InvoiceId, string Reason) : DomainEvent;
