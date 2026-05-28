using System.Text;
using System.Text.Json;
using AccountingService.Application.Commands.CreateLedgerEntry;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AccountingService.Infrastructure.Adapters.Inbound;

public class ReceivableCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ReceivableCreatedConsumer> _logger;
    private readonly string _bootstrapServers;

    public ReceivableCreatedConsumer(
        IServiceScopeFactory scopeFactory,
        ILogger<ReceivableCreatedConsumer> logger,
        string bootstrapServers)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _bootstrapServers = bootstrapServers;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "accounting-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("receivable.created");

        _logger.LogInformation("ReceivableCreatedConsumer started, listening to receivable.created");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result is null) continue;

                var correlationId = GetHeader(result.Message.Headers, "correlation-id");

                _logger.LogInformation(
                    "Received receivable.created message {Offset} correlationId {CorrelationId}",
                    result.Offset, correlationId);

                var payload = JsonSerializer.Deserialize<ReceivableCreatedPayload>(result.Message.Value);
                if (payload is null) continue;

                using var scope = _scopeFactory.CreateScope();
                var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                await mediator.Send(new CreateLedgerEntryCommand(
                    payload.ReceivableId,
                    payload.Amount,
                    payload.Currency,
                    correlationId
                ), stoppingToken);

                consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing receivable.created message");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        consumer.Close();
    }

    private static string GetHeader(Headers headers, string key)
    {
        var header = headers.FirstOrDefault(h => h.Key == key);
        return header is null ? string.Empty : Encoding.UTF8.GetString(header.GetValueBytes());
    }
}

public record ReceivableCreatedPayload(Guid ReceivableId, decimal Amount, string Currency);
