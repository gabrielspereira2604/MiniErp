using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FinancialService.Application.Commands.CreateReceivable;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FinancialService.Infrastructure.Adapters.Inbound;

public class InvoiceCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<InvoiceCreatedConsumer> _logger;
    private readonly string _bootstrapServers;

    public InvoiceCreatedConsumer(
        IServiceScopeFactory scopeFactory,
        ILogger<InvoiceCreatedConsumer> logger,
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
            GroupId = "financial-service",           // consumer group
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false                 // commit manual — só confirma após processar
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("invoice.created");

        _logger.LogInformation("InvoiceCreatedConsumer started, listening to invoice.created");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result is null) continue;

                var correlationId = GetHeader(result.Message.Headers, "correlation-id");

                _logger.LogInformation(
                    "Received invoice.created message {Offset} correlationId {CorrelationId}",
                    result.Offset, correlationId);

                var payload = JsonSerializer.Deserialize<InvoiceCreatedPayload>(result.Message.Value);
                if (payload is null) continue;

                using var scope = _scopeFactory.CreateScope();
                var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                await mediator.Send(new CreateReceivableCommand(
                    payload.InvoiceId,
                    payload.Amount,
                    payload.Currency,
                    correlationId
                ), stoppingToken);

                consumer.Commit(result);  // commit manual após processar com sucesso
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing invoice.created message");
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

public record InvoiceCreatedPayload(Guid InvoiceId, decimal Amount, string Currency);
