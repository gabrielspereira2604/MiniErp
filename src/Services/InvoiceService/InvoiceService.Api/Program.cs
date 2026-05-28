using InvoiceService.Api.Middleware;
using InvoiceService.Application.Commands.CreateInvoice;
using InvoiceService.Domain.Ports;
using InvoiceService.Infrastructure.Adapters.Outbound.Messaging;
using InvoiceService.Infrastructure.Adapters.Outbound.Outbox;
using InvoiceService.Infrastructure.Adapters.Outbound.Persistence;
using MediatR;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// MediatR
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(CreateInvoiceHandler).Assembly));

// EF Core + PostgreSQL
builder.Services.AddDbContext<InvoiceDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("InvoiceDb")));

// Repositório
builder.Services.AddScoped<IInvoiceRepository, InvoiceRepository>();

// Kafka Producer
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
builder.Services.AddSingleton<IEventPublisher>(sp =>
    new KafkaEventPublisher(kafkaBootstrapServers, sp.GetRequiredService<ILogger<KafkaEventPublisher>>()));

// Outbox Worker
builder.Services.AddHostedService<OutboxWorker>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseMiddleware<CorrelationIdMiddleware>();
app.UseHttpsRedirection();
app.MapControllers();

app.Run();
