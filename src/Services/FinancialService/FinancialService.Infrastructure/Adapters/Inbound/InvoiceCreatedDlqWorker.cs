using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FinancialService.Application.Commands.CreateReceivable;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FinancialService.Infrastructure.Adapters.Inbound;

public class InvoiceCreatedDlqWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<InvoiceCreatedDlqWorker> _logger;
    private readonly string _bootstrapServers;

    public InvoiceCreatedDlqWorker(
        IServiceScopeFactory scopeFactory,
        ILogger<InvoiceCreatedDlqWorker> logger,
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
            GroupId = "financial-service-dlq",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("invoice.created.dlq");

        _logger.LogInformation("InvoiceCreatedDlqWorker started, listening to invoice.created.dlq");

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

                var payload = JsonSerializer.Deserialize<InvoiceCreatedPayload>(result.Message.Value);

                if (payload is not null)
                {
                    using var scope = _scopeFactory.CreateScope();
                    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    await mediator.Send(new CreateReceivableCommand(
                        payload.InvoiceId,
                        payload.Amount,
                        payload.Currency,
                        correlationId
                    ), stoppingToken);

                    _logger.LogInformation(
                        "DLQ message reprocessed successfully — offset {Offset}", result.Offset);
                }

                // commita sempre — não volta para DLQ mesmo se falhar
                consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                // loga com máximo detalhe para investigação manual
                // commita mesmo assim — evita loop infinito na DLQ
                _logger.LogCritical(ex,
                    "DLQ message failed reprocessing — manual intervention required. " +
                    "Check invoice.created.dlq for stuck messages");

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
