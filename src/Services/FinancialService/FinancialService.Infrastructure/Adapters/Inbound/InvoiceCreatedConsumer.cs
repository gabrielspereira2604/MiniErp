using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FinancialService.Application.Commands.CreateReceivable;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using StackExchange.Redis;

namespace FinancialService.Infrastructure.Adapters.Inbound;

public class InvoiceCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<InvoiceCreatedConsumer> _logger;
    private readonly string _bootstrapServers;
    private readonly IConnectionMultiplexer _redis;
    private readonly ResiliencePipeline _pipeline;

    private const string DlqTopic = "invoice.created.dlq";
    private const int DlqThreshold = 3;

    public InvoiceCreatedConsumer(
        IServiceScopeFactory scopeFactory,
        ILogger<InvoiceCreatedConsumer> logger,
        string bootstrapServers,
        IConnectionMultiplexer redis)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _bootstrapServers = bootstrapServers;
        _redis = redis;
        _pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                MinimumThroughput = 5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = TimeSpan.FromSeconds(30),
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
                OnOpened = args =>
                {
                    _logger.LogError(
                        "Circuit breaker OPENED — invoice.created processing paused for {BreakDuration}s",
                        args.BreakDuration.TotalSeconds);
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    _logger.LogInformation("Circuit breaker CLOSED — invoice.created processing resumed");
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = args =>
                {
                    _logger.LogWarning("Circuit breaker HALF-OPEN — testing invoice.created processing");
                    return ValueTask.CompletedTask;
                }
            })
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 5,
                Delay = TimeSpan.FromSeconds(1),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not OperationCanceledException),
                OnRetry = args =>
                {
                    _logger.LogWarning(
                        "Retry {Attempt} processing invoice.created — {Exception}",
                        args.AttemptNumber, args.Outcome.Exception?.Message);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "financial-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("invoice.created");

        _logger.LogInformation("InvoiceCreatedConsumer started, listening to invoice.created");

        ConsumeResult<string, string>? result = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                result = consumer.Consume(stoppingToken);
                if (result is null) continue;

                var correlationId = GetHeader(result.Message.Headers, "correlation-id");

                _logger.LogInformation(
                    "Received invoice.created message {Offset} correlationId {CorrelationId}",
                    result.Offset, correlationId);

                var payload = JsonSerializer.Deserialize<InvoiceCreatedPayload>(result.Message.Value);
                if (payload is null) continue;

                await _pipeline.ExecuteAsync(async ct =>
                {
                    using var scope = _scopeFactory.CreateScope();
                    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    await mediator.Send(new CreateReceivableCommand(
                        payload.InvoiceId,
                        payload.Amount,
                        payload.Currency,
                        correlationId
                    ), ct);
                }, stoppingToken);

                consumer.Commit(result);

                // limpa contador de tentativas ao processar com sucesso
                await _redis.GetDatabase().KeyDeleteAsync($"dlq:attempts:financial:{result.TopicPartitionOffset}");
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (BrokenCircuitException)
            {
                // fallback: CB aberto — não commita o offset, Kafka reentrega quando o serviço voltar
                _logger.LogWarning(
                    "Fallback — circuit breaker open, invoice.created message will be redelivered by Kafka");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (Exception ex)
            {
                if (result is null) continue;

                var redisKey = $"dlq:attempts:financial:{result.TopicPartitionOffset}";
                var db = _redis.GetDatabase();
                var attempts = await db.StringIncrementAsync(redisKey);
                await db.KeyExpireAsync(redisKey, TimeSpan.FromDays(1));

                if (attempts >= DlqThreshold)
                {
                    await PublishToDlqAsync(result);
                    consumer.Commit(result);
                    await db.KeyDeleteAsync(redisKey);
                    _logger.LogError(ex,
                        "Message sent to DLQ after {Attempts} failed deliveries — offset {Offset}",
                        attempts, result.Offset);
                }
                else
                {
                    _logger.LogError(ex,
                        "Failed all retries ({Attempts}/{Threshold}) — Kafka will redeliver offset {Offset}",
                        attempts, DlqThreshold, result.Offset);
                }
            }
        }

        consumer.Close();
    }

    private async Task PublishToDlqAsync(ConsumeResult<string, string> original)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        await producer.ProduceAsync(DlqTopic, new Message<string, string>
        {
            Key = original.Message.Key,
            Value = original.Message.Value,
            Headers = original.Message.Headers
        });
    }

    private static string GetHeader(Headers headers, string key)
    {
        var header = headers.FirstOrDefault(h => h.Key == key);
        return header is null ? string.Empty : Encoding.UTF8.GetString(header.GetValueBytes());
    }
}

public record InvoiceCreatedPayload(Guid InvoiceId, decimal Amount, string Currency);
