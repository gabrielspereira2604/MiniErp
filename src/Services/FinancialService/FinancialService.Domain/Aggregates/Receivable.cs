using FinancialService.Domain.Events;
using FinancialService.Domain.ValueObjects;
using Shared.Kernel;

namespace FinancialService.Domain.Aggregates;

public enum ReceivableStatus { Pending, Received, Reversed }

public class Receivable : AggregateRoot
{
    public Guid InvoiceId { get; private set; }
    public Money Amount { get; private set; } = null!;
    public ReceivableStatus Status { get; private set; }
    public DateTime CreatedAt { get; private set; }
    public string? ReversalReason { get; private set; }

    private Receivable() { }

    public static Receivable Create(Guid id, Guid invoiceId, Money amount)
    {
        var receivable = new Receivable
        {
            Id = id,
            InvoiceId = invoiceId,
            Amount = amount,
            Status = ReceivableStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };

        receivable.RaiseDomainEvent(new ReceivableCreatedEvent(
            id, invoiceId, amount.Amount, amount.Currency));

        return receivable;
    }

    public void Reverse(string reason)
    {
        if (Status == ReceivableStatus.Reversed)
            throw new DomainException("Receivable já foi revertido");

        if (string.IsNullOrWhiteSpace(reason))
            throw new DomainException("Motivo da reversão é obrigatório");

        Status = ReceivableStatus.Reversed;
        ReversalReason = reason;

        RaiseDomainEvent(new ReceivableReversedEvent(Id, InvoiceId, reason));
    }
}
