using Shared.Kernel;

namespace InvoiceService.Domain.Events;

public record InvoiceCancelledEvent(Guid InvoiceId, string Reason) : DomainEvent;
