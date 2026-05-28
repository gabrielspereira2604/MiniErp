using Shared.Kernel;

namespace AccountingService.Domain.ValueObjects;

public record Money
{
    public decimal Amount { get; }
    public string Currency { get; }

    public Money(decimal amount, string currency)
    {
        if (amount <= 0)
            throw new DomainException("Amount deve ser maior que zero");

        if (string.IsNullOrWhiteSpace(currency))
            throw new DomainException("Currency é obrigatório");

        Amount = amount;
        Currency = currency.ToUpper();
    }
}
