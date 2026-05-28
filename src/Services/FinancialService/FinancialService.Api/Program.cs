using FinancialService.Application.Commands.CreateReceivable;
using FinancialService.Domain.Ports;
using FinancialService.Infrastructure.Adapters.Inbound;
using FinancialService.Infrastructure.Adapters.Outbound.Messaging;
using FinancialService.Infrastructure.Adapters.Outbound.Outbox;
using FinancialService.Infrastructure.Adapters.Outbound.Persistence;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// MediatR
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(CreateReceivableHandler).Assembly));

// EF Core + PostgreSQL
builder.Services.AddDbContext<FinancialDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("FinancialDb")));

// Repositório
builder.Services.AddScoped<IReceivableRepository, ReceivableRepository>();

// Kafka Producer
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
builder.Services.AddSingleton<IEventPublisher>(sp =>
    new KafkaEventPublisher(kafkaBootstrapServers, sp.GetRequiredService<ILogger<KafkaEventPublisher>>()));

// Inbound — Kafka Consumer
builder.Services.AddSingleton(kafkaBootstrapServers);
builder.Services.AddHostedService<InvoiceCreatedConsumer>();

// Outbound — Outbox Worker
builder.Services.AddHostedService<OutboxWorker>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();
app.MapControllers();

app.Run();
