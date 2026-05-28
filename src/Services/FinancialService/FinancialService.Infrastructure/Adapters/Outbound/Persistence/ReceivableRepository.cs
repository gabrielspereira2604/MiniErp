using System.Text.Json;
using FinancialService.Domain.Aggregates;
using FinancialService.Domain.Ports;
using FinancialService.Infrastructure.Adapters.Outbound.Outbox;
using Microsoft.EntityFrameworkCore;

namespace FinancialService.Infrastructure.Adapters.Outbound.Persistence;

public class ReceivableRepository : IReceivableRepository
{
    private readonly FinancialDbContext _context;

    public ReceivableRepository(FinancialDbContext context)
    {
        _context = context;
    }

    public async Task<Receivable?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.Receivables
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    }

    public async Task<Receivable?> GetByInvoiceIdAsync(Guid invoiceId, CancellationToken cancellationToken = default)
    {
        return await _context.Receivables
            .FirstOrDefaultAsync(x => x.InvoiceId == invoiceId, cancellationToken);
    }

    public async Task SaveAsync(Receivable receivable, CancellationToken cancellationToken = default)
    {
        var isNew = !await _context.Receivables.AnyAsync(x => x.Id == receivable.Id, cancellationToken);

        if (isNew)
            await _context.Receivables.AddAsync(receivable, cancellationToken);
        else
            _context.Receivables.Update(receivable);

        foreach (var domainEvent in receivable.DomainEvents)
        {
            await _context.OutboxMessages.AddAsync(new OutboxMessage
            {
                Topic = domainEvent is Domain.Events.ReceivableCreatedEvent
                    ? "receivable.created"
                    : "receivable.reversed",
                Payload = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
                CorrelationId = domainEvent.CorrelationId
            }, cancellationToken);
        }

        await _context.SaveChangesAsync(cancellationToken);
        receivable.ClearDomainEvents();
    }
}
