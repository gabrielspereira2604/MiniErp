using System.Text;
using Confluent.Kafka;
using FinancialService.Domain.Ports;
using Microsoft.Extensions.Logging;

namespace FinancialService.Infrastructure.Adapters.Outbound.Messaging;

public class KafkaEventPublisher : IEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaEventPublisher> _logger;

    public KafkaEventPublisher(string bootstrapServers, ILogger<KafkaEventPublisher> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5,
            RetryBackoffMs = 1000
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync(string topic, string payload, string correlationId, CancellationToken cancellationToken = default)
    {
        var message = new Message<string, string>
        {
            Key = correlationId,
            Value = payload,
            Headers = new Headers
            {
                { "correlation-id", Encoding.UTF8.GetBytes(correlationId) },
                { "published-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) }
            }
        };

        var result = await _producer.ProduceAsync(topic, message, cancellationToken);

        _logger.LogInformation(
            "Event published to topic {Topic} partition {Partition} offset {Offset}",
            result.Topic, result.Partition, result.Offset);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
