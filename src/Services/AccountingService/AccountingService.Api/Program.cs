using AccountingService.Application.Commands.CreateLedgerEntry;
using AccountingService.Domain.Ports;
using AccountingService.Infrastructure.Adapters.Inbound;
using AccountingService.Infrastructure.Adapters.Outbound.Messaging;
using AccountingService.Infrastructure.Adapters.Outbound.Outbox;
using AccountingService.Infrastructure.Adapters.Outbound.Persistence;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// MediatR
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(CreateLedgerEntryHandler).Assembly));

// EF Core + PostgreSQL
builder.Services.AddDbContext<AccountingDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("AccountingDb")));

// Repositório
builder.Services.AddScoped<ILedgerEntryRepository, LedgerEntryRepository>();

// Kafka Producer
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
builder.Services.AddSingleton<IEventPublisher>(sp =>
    new KafkaEventPublisher(kafkaBootstrapServers, sp.GetRequiredService<ILogger<KafkaEventPublisher>>()));

// Inbound — Kafka Consumer
builder.Services.AddSingleton(kafkaBootstrapServers);
builder.Services.AddHostedService<ReceivableCreatedConsumer>();

// Outbound — Outbox Worker
builder.Services.AddHostedService<OutboxWorker>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();
app.MapControllers();

app.Run();
