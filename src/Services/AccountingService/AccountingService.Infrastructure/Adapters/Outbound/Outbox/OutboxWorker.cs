using AccountingService.Domain.Ports;
using AccountingService.Infrastructure.Adapters.Outbound.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AccountingService.Infrastructure.Adapters.Outbound.Outbox;

public class OutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxWorker> _logger;

    public OutboxWorker(IServiceScopeFactory scopeFactory, ILogger<OutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ProcessPendingMessagesAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task ProcessPendingMessagesAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AccountingDbContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        var pending = await context.OutboxMessages
            .Where(x => x.PublishedAt == null)
            .OrderBy(x => x.CreatedAt)
            .Take(50)
            .ToListAsync(ct);

        if (!pending.Any()) return;

        foreach (var message in pending)
        {
            try
            {
                await publisher.PublishAsync(
                    message.Topic,
                    message.Payload,
                    message.CorrelationId,
                    ct);

                message.PublishedAt = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to publish outbox message {MessageId} to topic {Topic}",
                    message.Id, message.Topic);
            }
        }

        await context.SaveChangesAsync(ct);
    }
}
