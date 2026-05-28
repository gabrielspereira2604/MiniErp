using FluentAssertions;
using InvoiceService.Domain.Aggregates;
using InvoiceService.Domain.Events;
using InvoiceService.Domain.ValueObjects;
using Shared.Kernel;

namespace InvoiceService.UnitTests.Domain;

public class InvoiceTests
{
    private static readonly Money ValidAmount = new(100, "BRL");
    private const string ValidDocument = "12345678900";

    [Fact]
    public void Create_ValidData_ShouldCreatePendingInvoice()
    {
        var id = Guid.NewGuid();

        var invoice = Invoice.Create(id, ValidAmount, ValidDocument);

        invoice.Id.Should().Be(id);
        invoice.Status.Should().Be(InvoiceStatus.Pending);
        invoice.CustomerDocument.Should().Be(ValidDocument);
        invoice.Amount.Should().Be(ValidAmount);
    }

    [Fact]
    public void Create_ShouldRaiseInvoiceCreatedEvent()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);

        invoice.DomainEvents.Should().ContainSingle()
            .Which.Should().BeOfType<InvoiceCreatedEvent>();
    }

    [Fact]
    public void Create_EmptyDocument_ShouldThrowDomainException()
    {
        var act = () => Invoice.Create(Guid.NewGuid(), ValidAmount, "");

        act.Should().Throw<DomainException>()
            .WithMessage("Documento do cliente é obrigatório");
    }

    [Fact]
    public void Confirm_PendingInvoice_ShouldConfirm()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);

        invoice.Confirm();

        invoice.Status.Should().Be(InvoiceStatus.Confirmed);
    }

    [Fact]
    public void Confirm_CancelledInvoice_ShouldThrowDomainException()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);
        invoice.Cancel("motivo");

        var act = () => invoice.Confirm();

        act.Should().Throw<DomainException>()
            .WithMessage("Apenas notas pendentes podem ser confirmadas");
    }

    [Fact]
    public void Cancel_PendingInvoice_ShouldCancel()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);

        invoice.Cancel("cliente solicitou");

        invoice.Status.Should().Be(InvoiceStatus.Cancelled);
        invoice.CancellationReason.Should().Be("cliente solicitou");
    }

    [Fact]
    public void Cancel_ShouldRaiseInvoiceCancelledEvent()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);
        invoice.ClearDomainEvents();

        invoice.Cancel("motivo");

        invoice.DomainEvents.Should().ContainSingle()
            .Which.Should().BeOfType<InvoiceCancelledEvent>();
    }

    [Fact]
    public void Cancel_AlreadyCancelled_ShouldThrowDomainException()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);
        invoice.Cancel("primeiro cancelamento");

        var act = () => invoice.Cancel("segundo cancelamento");

        act.Should().Throw<DomainException>()
            .WithMessage("Nota já foi cancelada");
    }

    [Fact]
    public void Cancel_EmptyReason_ShouldThrowDomainException()
    {
        var invoice = Invoice.Create(Guid.NewGuid(), ValidAmount, ValidDocument);

        var act = () => invoice.Cancel("");

        act.Should().Throw<DomainException>()
            .WithMessage("Motivo do cancelamento é obrigatório");
    }
}
