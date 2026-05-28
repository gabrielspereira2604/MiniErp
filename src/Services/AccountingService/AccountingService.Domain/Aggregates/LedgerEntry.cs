using AccountingService.Domain.Events;
using AccountingService.Domain.ValueObjects;
using Shared.Kernel;

namespace AccountingService.Domain.Aggregates;

public enum LedgerEntryStatus { Posted, Reversed }

public class LedgerEntry : AggregateRoot
{
    public Guid ReceivableId { get; private set; }
    public Money Amount { get; private set; } = null!;
    public string DebitAccount { get; private set; } = null!;
    public string CreditAccount { get; private set; } = null!;
    public LedgerEntryStatus Status { get; private set; }
    public DateTime CreatedAt { get; private set; }
    public string? ReversalReason { get; private set; }

    private LedgerEntry() { }

    public static LedgerEntry Create(Guid id, Guid receivableId, Money amount)
    {
        var entry = new LedgerEntry
        {
            Id = id,
            ReceivableId = receivableId,
            Amount = amount,
            DebitAccount = "1.1.1 - Contas a Receber",
            CreditAccount = "3.1.1 - Receita de Vendas",
            Status = LedgerEntryStatus.Posted,
            CreatedAt = DateTime.UtcNow
        };

        entry.RaiseDomainEvent(new LedgerEntryCreatedEvent(
            id, receivableId, amount.Amount, amount.Currency,
            entry.DebitAccount, entry.CreditAccount));

        return entry;
    }

    public void Reverse(string reason)
    {
        if (Status == LedgerEntryStatus.Reversed)
            throw new DomainException("LedgerEntry já foi revertido");

        if (string.IsNullOrWhiteSpace(reason))
            throw new DomainException("Motivo da reversão é obrigatório");

        Status = LedgerEntryStatus.Reversed;
        ReversalReason = reason;

        RaiseDomainEvent(new LedgerEntryReversedEvent(Id, ReceivableId, reason));
    }
}
