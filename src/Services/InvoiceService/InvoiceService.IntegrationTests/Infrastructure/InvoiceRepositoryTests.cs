using FluentAssertions;
using InvoiceService.Domain.Aggregates;
using InvoiceService.Domain.ValueObjects;
using InvoiceService.Infrastructure.Adapters.Outbound.Persistence;
using InvoiceService.IntegrationTests.Fixtures;

namespace InvoiceService.IntegrationTests.Infrastructure;

public class InvoiceRepositoryTests : IClassFixture<PostgreSqlFixture>
{
    private readonly PostgreSqlFixture _fixture;

    public InvoiceRepositoryTests(PostgreSqlFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SaveAsync_NewInvoice_ShouldPersist()
    {
        await using var context = _fixture.CreateDbContext();
        var repository = new InvoiceRepository(context);

        var invoice = Invoice.Create(Guid.NewGuid(), new Money(500, "BRL"), "12345678900");
        await repository.SaveAsync(invoice);

        var saved = await repository.GetByIdAsync(invoice.Id);
        saved.Should().NotBeNull();
        saved!.Amount.Amount.Should().Be(500);
        saved.Status.Should().Be(InvoiceStatus.Pending);
    }

    [Fact]
    public async Task SaveAsync_NewInvoice_ShouldSaveOutboxMessage()
    {
        await using var context = _fixture.CreateDbContext();
        var repository = new InvoiceRepository(context);

        var invoice = Invoice.Create(Guid.NewGuid(), new Money(200, "BRL"), "12345678900");
        await repository.SaveAsync(invoice);

        var outboxMessages = context.OutboxMessages.Where(x => x.PublishedAt == null).ToList();
        outboxMessages.Should().NotBeEmpty();
    }

    [Fact]
    public async Task SaveAsync_CancelledInvoice_ShouldUpdateStatus()
    {
        await using var context = _fixture.CreateDbContext();
        var repository = new InvoiceRepository(context);

        var invoice = Invoice.Create(Guid.NewGuid(), new Money(300, "BRL"), "12345678900");
        await repository.SaveAsync(invoice);

        invoice.Cancel("cliente solicitou");
        await repository.SaveAsync(invoice);

        var updated = await repository.GetByIdAsync(invoice.Id);
        updated!.Status.Should().Be(InvoiceStatus.Cancelled);
        updated.CancellationReason.Should().Be("cliente solicitou");
    }

    [Fact]
    public async Task SaveAsync_ShouldClearDomainEventsAfterSave()
    {
        await using var context = _fixture.CreateDbContext();
        var repository = new InvoiceRepository(context);

        var invoice = Invoice.Create(Guid.NewGuid(), new Money(100, "BRL"), "12345678900");
        await repository.SaveAsync(invoice);

        invoice.DomainEvents.Should().BeEmpty();
    }

    [Fact]
    public async Task GetByIdAsync_NonExistentId_ShouldReturnNull()
    {
        await using var context = _fixture.CreateDbContext();
        var repository = new InvoiceRepository(context);

        var result = await repository.GetByIdAsync(Guid.NewGuid());

        result.Should().BeNull();
    }
}
