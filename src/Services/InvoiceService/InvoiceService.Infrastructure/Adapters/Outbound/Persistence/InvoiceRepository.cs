using System.Text.Json;
using InvoiceService.Domain.Aggregates;
using InvoiceService.Domain.Ports;
using InvoiceService.Infrastructure.Adapters.Outbound.Outbox;
using Microsoft.EntityFrameworkCore;

namespace InvoiceService.Infrastructure.Adapters.Outbound.Persistence;

public class InvoiceRepository : IInvoiceRepository
{
    private readonly InvoiceDbContext _context;

    public InvoiceRepository(InvoiceDbContext context)
    {
        _context = context;
    }

    public async Task<Invoice?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.Invoices
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    }

    public async Task SaveAsync(Invoice invoice, CancellationToken cancellationToken = default)
    {
        var isNew = !await _context.Invoices.AnyAsync(x => x.Id == invoice.Id, cancellationToken);

        if (isNew)
            await _context.Invoices.AddAsync(invoice, cancellationToken);
        else
            _context.Invoices.Update(invoice);

        // Outbox — salva eventos na mesma transação
        foreach (var domainEvent in invoice.DomainEvents)
        {
            await _context.OutboxMessages.AddAsync(new OutboxMessage
            {
                Topic = domainEvent.GetType().Name
                    .Replace("Event", string.Empty)
                    .ToLower()
                    .Replace("invoicecreated", "invoice.created")
                    .Replace("invoicecancelled", "invoice.cancelled"),
                Payload = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
                CorrelationId = domainEvent.CorrelationId
            }, cancellationToken);
        }

        await _context.SaveChangesAsync(cancellationToken);

        invoice.ClearDomainEvents();
    }
}
