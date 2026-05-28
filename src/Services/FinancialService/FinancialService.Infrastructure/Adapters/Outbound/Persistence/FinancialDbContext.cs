using FinancialService.Domain.Aggregates;
using FinancialService.Infrastructure.Adapters.Outbound.Outbox;
using FinancialService.Infrastructure.Adapters.Outbound.Persistence.Configurations;
using Microsoft.EntityFrameworkCore;

namespace FinancialService.Infrastructure.Adapters.Outbound.Persistence;

public class FinancialDbContext : DbContext
{
    public FinancialDbContext(DbContextOptions<FinancialDbContext> options) : base(options) { }

    public DbSet<Receivable> Receivables => Set<Receivable>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new ReceivableConfiguration());

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
