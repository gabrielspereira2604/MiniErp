using Shared.Kernel;

namespace InvoiceService.Domain.ValueObjects;

public record Money
{
    public decimal Amount { get; }
    public string Currency { get; }

    public Money(decimal amount, string currency = "BRL")
    {
        if (amount <= 0)
            throw new DomainException("Valor deve ser maior que zero");

        if (string.IsNullOrWhiteSpace(currency))
            throw new DomainException("Moeda é obrigatória");

        Amount = amount;
        Currency = currency.ToUpper();
    }

    public override string ToString() => $"{Currency} {Amount:F2}";
}
