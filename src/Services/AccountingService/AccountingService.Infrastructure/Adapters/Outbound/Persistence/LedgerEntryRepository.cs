using System.Text.Json;
using AccountingService.Domain.Aggregates;
using AccountingService.Domain.Ports;
using AccountingService.Infrastructure.Adapters.Outbound.Outbox;
using Microsoft.EntityFrameworkCore;

namespace AccountingService.Infrastructure.Adapters.Outbound.Persistence;

public class LedgerEntryRepository : ILedgerEntryRepository
{
    private readonly AccountingDbContext _context;

    public LedgerEntryRepository(AccountingDbContext context)
    {
        _context = context;
    }

    public async Task<LedgerEntry?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.LedgerEntries
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    }

    public async Task<LedgerEntry?> GetByReceivableIdAsync(Guid receivableId, CancellationToken cancellationToken = default)
    {
        return await _context.LedgerEntries
            .FirstOrDefaultAsync(x => x.ReceivableId == receivableId, cancellationToken);
    }

    public async Task SaveAsync(LedgerEntry ledgerEntry, CancellationToken cancellationToken = default)
    {
        var isNew = !await _context.LedgerEntries.AnyAsync(x => x.Id == ledgerEntry.Id, cancellationToken);

        if (isNew)
            await _context.LedgerEntries.AddAsync(ledgerEntry, cancellationToken);
        else
            _context.LedgerEntries.Update(ledgerEntry);

        foreach (var domainEvent in ledgerEntry.DomainEvents)
        {
            await _context.OutboxMessages.AddAsync(new OutboxMessage
            {
                Topic = domainEvent is Domain.Events.LedgerEntryCreatedEvent
                    ? "ledger.entry.created"
                    : "ledger.entry.reversed",
                Payload = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
                CorrelationId = domainEvent.CorrelationId
            }, cancellationToken);
        }

        await _context.SaveChangesAsync(cancellationToken);
        ledgerEntry.ClearDomainEvents();
    }
}
