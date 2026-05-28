using InvoiceService.Domain.Events;
using InvoiceService.Domain.ValueObjects;
using Shared.Kernel;

namespace InvoiceService.Domain.Aggregates;

public enum InvoiceStatus { Pending, Confirmed, Cancelled }

public class Invoice : AggregateRoot
{
    public Money Amount { get; private set; } = null!;
    public InvoiceStatus Status { get; private set; }
    public string CustomerDocument { get; private set; } = null!;
    public DateTime CreatedAt { get; private set; }
    public string? CancellationReason { get; private set; }

    private Invoice() { }

    public static Invoice Create(Guid id, Money amount, string customerDocument)
    {
        if (string.IsNullOrWhiteSpace(customerDocument))
            throw new DomainException("Documento do cliente é obrigatório");

        var invoice = new Invoice
        {
            Id = id,
            Amount = amount,
            CustomerDocument = customerDocument,
            Status = InvoiceStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };

        invoice.RaiseDomainEvent(new InvoiceCreatedEvent(id, amount.Amount, amount.Currency));

        return invoice;
    }

    public void Confirm()
    {
        if (Status != InvoiceStatus.Pending)
            throw new DomainException("Apenas notas pendentes podem ser confirmadas");

        Status = InvoiceStatus.Confirmed;
    }

    public void Cancel(string reason)
    {
        if (Status == InvoiceStatus.Cancelled)
            throw new DomainException("Nota já foi cancelada");

        if (string.IsNullOrWhiteSpace(reason))
            throw new DomainException("Motivo do cancelamento é obrigatório");

        Status = InvoiceStatus.Cancelled;
        CancellationReason = reason;

        RaiseDomainEvent(new InvoiceCancelledEvent(Id, reason));
    }
}
