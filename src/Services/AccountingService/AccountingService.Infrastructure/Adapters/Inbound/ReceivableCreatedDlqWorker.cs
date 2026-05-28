using System.Text;
using System.Text.Json;
using AccountingService.Application.Commands.CreateLedgerEntry;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AccountingService.Infrastructure.Adapters.Inbound;

public class ReceivableCreatedDlqWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ReceivableCreatedDlqWorker> _logger;
    private readonly string _bootstrapServers;

    public ReceivableCreatedDlqWorker(
        IServiceScopeFactory scopeFactory,
        ILogger<ReceivableCreatedDlqWorker> logger,
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
            GroupId = "accounting-service-dlq",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("receivable.created.dlq");

        _logger.LogInformation("ReceivableCreatedDlqWorker started, listening to receivable.created.dlq");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result is null) continue;

                var correlationId = GetHeader(result.Message.Headers, "correlation-id");

                _logger.LogWarning(
                    "Processing DLQ message offset {Offset} correlationId {CorrelationId}",
                    result.Offset, correlationId);

                var payload = JsonSerializer.Deserialize<ReceivableCreatedPayload>(result.Message.Value);

                if (payload is not null)
                {
                    using var scope = _scopeFactory.CreateScope();
                    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    await mediator.Send(new CreateLedgerEntryCommand(
                        payload.ReceivableId,
                        payload.Amount,
                        payload.Currency,
                        correlationId
                    ), stoppingToken);

                    _logger.LogInformation(
                        "DLQ message reprocessed successfully — offset {Offset}", result.Offset);
                }

                consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex,
                    "DLQ message failed reprocessing — manual intervention required. " +
                    "Check receivable.created.dlq for stuck messages");

                try { consumer.Commit(); } catch { }
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
