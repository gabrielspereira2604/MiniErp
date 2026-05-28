using FluentAssertions;
using InvoiceService.Domain.ValueObjects;
using Shared.Kernel;

namespace InvoiceService.UnitTests.Domain;

public class MoneyTests
{
    [Fact]
    public void Create_ValidAmount_ShouldCreateMoney()
    {
        var money = new Money(100, "BRL");

        money.Amount.Should().Be(100);
        money.Currency.Should().Be("BRL");
    }

    [Fact]
    public void Create_DefaultCurrency_ShouldBeBRL()
    {
        var money = new Money(100);

        money.Currency.Should().Be("BRL");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Create_InvalidAmount_ShouldThrowDomainException(decimal amount)
    {
        var act = () => new Money(amount);

        act.Should().Throw<DomainException>()
            .WithMessage("Valor deve ser maior que zero");
    }

    [Fact]
    public void Create_EmptyCurrency_ShouldThrowDomainException()
    {
        var act = () => new Money(100, "");

        act.Should().Throw<DomainException>()
            .WithMessage("Moeda é obrigatória");
    }
}
