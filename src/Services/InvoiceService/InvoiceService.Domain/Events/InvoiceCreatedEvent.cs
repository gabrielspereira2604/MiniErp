using Shared.Kernel;

namespace InvoiceService.Domain.Events;

public record InvoiceCreatedEvent(Guid InvoiceId, decimal Amount, string Currency) : DomainEvent;
