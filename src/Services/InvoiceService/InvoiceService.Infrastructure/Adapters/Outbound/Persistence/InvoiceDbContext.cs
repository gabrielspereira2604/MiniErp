using InvoiceService.Domain.Aggregates;
using InvoiceService.Infrastructure.Adapters.Outbound.Outbox;
using InvoiceService.Infrastructure.Adapters.Outbound.Persistence.Configurations;
using Microsoft.EntityFrameworkCore;

namespace InvoiceService.Infrastructure.Adapters.Outbound.Persistence;

public class InvoiceDbContext : DbContext
{
    public InvoiceDbContext(DbContextOptions<InvoiceDbContext> options) : base(options) { }

    public DbSet<Invoice> Invoices => Set<Invoice>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new InvoiceConfiguration());

        modelBuilder.Entity<OutboxMessage>(builder =>
        {
            builder.ToTable("outbox_messages");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.Topic).HasMaxLength(200).IsRequired();
            builder.Property(x => x.Payload).IsRequired();
            builder.Property(x => x.CorrelationId).HasMaxLength(100);
        });
    }
}
