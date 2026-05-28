using AccountingService.Domain.Aggregates;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AccountingService.Infrastructure.Adapters.Outbound.Persistence.Configurations;

public class LedgerEntryConfiguration : IEntityTypeConfiguration<LedgerEntry>
{
    public void Configure(EntityTypeBuilder<LedgerEntry> builder)
    {
        builder.ToTable("ledger_entries");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id).HasColumnName("id");
        builder.Property(x => x.ReceivableId).HasColumnName("receivable_id");

        builder.Property(x => x.DebitAccount)
            .HasColumnName("debit_account")
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(x => x.CreditAccount)
            .HasColumnName("credit_account")
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(x => x.Status)
            .HasColumnName("status")
            .HasConversion<string>();

        builder.Property(x => x.CreatedAt).HasColumnName("created_at");

        builder.Property(x => x.ReversalReason)
            .HasColumnName("reversal_reason")
            .HasMaxLength(500);

        builder.OwnsOne(x => x.Amount, money =>
        {
            money.Property(m => m.Amount).HasColumnName("amount").HasPrecision(18, 2);
            money.Property(m => m.Currency).HasColumnName("currency").HasMaxLength(3);
        });

        builder.Ignore(x => x.DomainEvents);
    }
}
