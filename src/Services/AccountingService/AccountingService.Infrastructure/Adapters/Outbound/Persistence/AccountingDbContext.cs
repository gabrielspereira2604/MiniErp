using AccountingService.Domain.Aggregates;
using AccountingService.Infrastructure.Adapters.Outbound.Outbox;
using AccountingService.Infrastructure.Adapters.Outbound.Persistence.Configurations;
using Microsoft.EntityFrameworkCore;

namespace AccountingService.Infrastructure.Adapters.Outbound.Persistence;

public class AccountingDbContext : DbContext
{
    public AccountingDbContext(DbContextOptions<AccountingDbContext> options) : base(options) { }

    public DbSet<LedgerEntry> LedgerEntries => Set<LedgerEntry>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new LedgerEntryConfiguration());

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
