using InvoiceService.Infrastructure.Adapters.Outbound.Persistence;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;

namespace InvoiceService.IntegrationTests.Fixtures;

public class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("invoicedb")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    public InvoiceDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<InvoiceDbContext>()
            .UseNpgsql(_container.GetConnectionString())
            .Options;

        var context = new InvoiceDbContext(options);
        context.Database.EnsureCreated();
        return context;
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
