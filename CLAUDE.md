# MiniErp — Microservices Training Project

## Objetivo

Projeto de treinamento para entrevistas técnicas de nível sênior em bancos como **BTG Pactual, XP Investimentos e Pine**.
Simula um mini-ERP com domínios de **Fiscal (Invoice), Financeiro e Contabilidade** para cobrir os patterns mais cobrados nesse mercado.

## Contexto do desenvolvedor

- Experiência com **Clean Architecture** e **N-Layer**
- Aprendendo **Hexagonal Architecture com DDD** no contexto deste projeto
- Linguagem principal: **C# / .NET 9**
- Objetivo: dominar os patterns abaixo a nível de entrevista técnica sênior

---

## Stack

| Camada | Tecnologia |
|---|---|
| Runtime | .NET 9 / ASP.NET Core 9 |
| Mensageria | Apache Kafka (Confluent.Kafka — direto, sem abstração) |
| Resilience | Polly v8 (`ResiliencePipeline`) |
| ORM (write) | EF Core 8 + PostgreSQL (Npgsql) |
| Query (read) | Dapper (CQRS read side) |
| Cache / Idempotência | Redis (`StackExchange.Redis`) |
| Mediator / CQRS | MediatR |
| Validação | FluentValidation |
| Tracing | OpenTelemetry .NET SDK + Datadog exporter |
| Testes | xUnit + FluentAssertions + Testcontainers |
| Local infra | Docker Compose |
| Cloud | AWS ECS Fargate / EKS (Kubernetes + Helm) |
| IaC | Terraform |

---

## Arquitetura por serviço: Hexagonal + DDD

Cada serviço segue esta estrutura (Clean Architecture com nomes de Hexagonal):

```
ServiceName.Domain/
  Aggregates/        ← Aggregate Roots com regras de negócio puras
  Events/            ← Domain Events (algo que aconteceu, imutável)
  ValueObjects/      ← Sem identidade, comparados por valor, imutáveis
  Exceptions/        ← DomainException
  Ports/             ← Interfaces que o domínio define (IRepository, IEventPublisher)

ServiceName.Application/
  Commands/          ← Command + Handler (MediatR) — write side
  Queries/           ← Query + Handler (MediatR + Dapper) — read side
  Behaviors/         ← MediatR pipeline behaviors (validation, idempotency, logging)

ServiceName.Infrastructure/
  Adapters/
    Outbound/
      Persistence/   ← EF Core DbContext, Repositories (implementam ports)
      Messaging/     ← Kafka Producer (implementa IEventPublisher)
      Outbox/        ← OutboxMessage entity + OutboxWorker (IHostedService)
    Inbound/         ← Kafka Consumers (se o serviço consome comandos via Kafka)

ServiceName.Api/
  Controllers/       ← Inbound HTTP adapters
  Middleware/        ← CorrelationId, Idempotency, Exception handling
```

**Regra de dependência:**
```
Api → Application → Domain        (permitido)
Infrastructure → Application      (permitido)
Domain → nada externo             (NUNCA depende de EF, Kafka, Redis)
```

---

## Serviços

### InvoiceService
- Emite e cancela notas fiscais (NF-e)
- **Porta de entrada:** HTTP POST /invoices
- **Eventos publicados:** `invoice.created`, `invoice.cancelled`
- **Aggregate:** `Invoice` com status `Pending → Confirmed → Cancelled`

### FinancialService
- Gerencia contas a receber/pagar
- **Consome:** `invoice.created` → cria `Receivable`
- **Eventos publicados:** `receivable.created`, `receivable.reversed`
- **Aggregate:** `Receivable`

### AccountingService
- Lançamentos no razão contábil (double-entry accounting)
- **Consome:** `receivable.created` → cria `LedgerEntry`
- **Eventos publicados:** `ledger.entry.created`, `ledger.entry.reversed`
- **Aggregate:** `LedgerEntry`

### SagaOrchestrator
- State machine que orquestra o fluxo distribuído
- **Saga:** InvoiceCreated → CreateReceivable → CreateLedgerEntry → Confirm
- **Compensação:** falha em qualquer step → reverter na ordem inversa
- **Implementação:** Worker (IHostedService) consumindo tópico `saga.commands`

---

## Kafka Topics

```
invoice.commands       → CreateInvoice, CancelInvoice
invoice.events         → InvoiceCreated, InvoiceCancelled

financial.commands     → CreateReceivable, ReverseReceivable
financial.events       → ReceivableCreated, ReceivableReversed

accounting.commands    → CreateLedgerEntry, ReverseLedgerEntry
accounting.events      → LedgerEntryCreated, LedgerEntryReversed

saga.commands          → StartInvoiceSaga, CompensateInvoiceSaga
saga.events            → SagaCompleted, SagaFailed

*.dlq                  → Dead Letter Queue de cada tópico (ex: invoice.commands.dlq)
```

---

## Patterns a implementar (ordem de prioridade)

### Fase 1 — Core (mais cobrado em entrevista)

#### 1. Transactional Outbox Pattern
O pattern mais cobrado. Garante entrega de eventos sem 2PC.

```
// ERRADO — race condition: banco salva mas Kafka pode cair antes de publicar
await db.SaveAsync(invoice);
await kafka.PublishAsync("invoice.created", event);

// CERTO — Outbox
BEGIN TRANSACTION
  INSERT INTO invoices (...)
  INSERT INTO outbox_messages (topic, payload, published = false)
COMMIT

// OutboxWorker (IHostedService) — processo separado
SELECT * FROM outbox_messages WHERE published = false
  → publica no Kafka
  → UPDATE published = true
```

Localização: `InvoiceService.Infrastructure/Adapters/Outbound/Outbox/`

#### 2. Idempotency Key
Evita processar o mesmo comando duas vezes (crítico em pagamentos).

```
// Header: x-idempotency-key: <uuid-do-cliente>
// Redis key: idempotency:{service}:{key}
// TTL: 24h
// Se key existe → retorna resultado cacheado, não reprocessa
```

Localização: `InvoiceService.Api/Middleware/IdempotencyMiddleware.cs`
Shared: `Shared.Idempotency/`

#### 3. Correlation ID
UUID propagado em todos os requests e mensagens para rastrear o fluxo completo.

```
// HTTP: header x-correlation-id
// Kafka: message header correlation-id
// Logs: sempre logar correlation-id
```

Localização: `InvoiceService.Api/Middleware/CorrelationIdMiddleware.cs`
Shared: `Shared.Tracing/`

### Fase 2 — Resiliência

#### 4. Retry com Exponential Backoff + Jitter (Polly v8)
```csharp
ResiliencePipelineBuilder
  .AddRetry(new RetryStrategyOptions
  {
      MaxRetryAttempts = 5,
      Delay = TimeSpan.FromSeconds(1),
      BackoffType = DelayBackoffType.Exponential,
      UseJitter = true   // evita thundering herd
  })
```

#### 5. Circuit Breaker distribuído (Polly + Redis)
- Estado do CB salvo no Redis (não in-process) — funciona com múltiplas instâncias
- Estados: `Closed → Open → Half-Open`
- Open: falha imediata sem chamar o serviço downstream
- Half-Open: deixa passar uma requisição de teste

#### 6. Fallback
Resposta degradada quando o circuit breaker está aberto.

#### 7. Dead Letter Queue (DLQ)
Mensagens que falharam N vezes vão para `*.dlq`.
- Worker separado processa DLQ para alertas/reprocessamento manual.

### Fase 3 — Saga Pattern

#### 8. Saga Orchestration (State Machine)
```
Estado da Saga:
  Started
  → WaitingReceivable
  → WaitingLedgerEntry  
  → Completed
  
  (em falha):
  → CompensatingLedger
  → CompensatingReceivable
  → CompensatingInvoice
  → Failed
```

#### 9. Compensation Transaction
Cada step tem uma operação de compensação idempotente:
- `CreateReceivable` → compensação: `ReverseReceivable`
- `CreateLedgerEntry` → compensação: `ReverseLedgerEntry`
- `CreateInvoice` → compensação: `CancelInvoice`

#### 10. Deduplicação de mensagens
- Consumer verifica `message-id` no Redis antes de processar
- Garante at-least-once delivery sem processar duplicatas

### Fase 4 — Event Sourcing

#### 11. Event Store (append-only)
```sql
CREATE TABLE domain_events (
  id UUID PRIMARY KEY,
  aggregate_id UUID NOT NULL,
  aggregate_type VARCHAR(100) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  version INT NOT NULL
);
```
- Estado atual derivado pelo replay de eventos
- Audit trail completo (requisito regulatório em fintechs)

### Fase 5 — Observabilidade

#### 12. OpenTelemetry + Distributed Tracing
- `ActivitySource` para criar spans customizados
- Trace context propagado via headers HTTP e Kafka
- Exportado para Datadog APM
- Cada serviço aparece como um span no trace do Datadog

#### 13. Métricas (CloudWatch + Datadog)
- Latência de processamento de mensagens
- Taxa de erro por tópico
- Tamanho da fila do Outbox
- Estado do Circuit Breaker

### Fase 6 — Infra

#### 14. Docker Compose (local)
Sobe: Kafka + Zookeeper, PostgreSQL (um por serviço), Redis, Kafka UI, Datadog Agent

#### 15. Kubernetes (EKS) + Helm
- Deployment, Service, HPA (horizontal pod autoscaler)
- ConfigMap para variáveis de ambiente
- Secrets para credenciais

#### 16. Terraform
- ECS Fargate task definitions ou EKS node groups
- RDS PostgreSQL, ElastiCache Redis, MSK (Kafka gerenciado)

---

## Perguntas de entrevista que este projeto responde

| Pergunta | Pattern / Código |
|---|---|
| "Como garantir entrega sem 2PC?" | Transactional Outbox |
| "Como evitar duplicar um lançamento?" | Idempotency Key + Redis |
| "Como reverter uma transação distribuída?" | Saga + Compensation |
| "O que é exactly-once em Kafka?" | Kafka Transactions + Deduplication |
| "Quais os estados de um Circuit Breaker?" | Polly v8 CB distribuído |
| "Como rastrear um erro em produção?" | Correlation ID + OpenTelemetry + Datadog |
| "Como funciona auditoria em fintech?" | Event Sourcing |
| "O que acontece quando um consumer falha 5x?" | DLQ |
| "Como evitar thundering herd?" | Jitter no retry |
| "Diferença entre at-least-once e exactly-once?" | Kafka consumer semantics |
| "O que é CQRS e quando usar?" | Commands com EF, Queries com Dapper |

---

## Próximo passo sugerido

Implementar **InvoiceService completo** na seguinte ordem:
1. `Shared.Kernel` — base classes (AggregateRoot, DomainEvent, DomainException)
2. `InvoiceService.Domain` — Invoice aggregate, Money value object, domain events, ports
3. `InvoiceService.Application` — CreateInvoice command handler, MediatR behaviors
4. `InvoiceService.Infrastructure` — EF Core, Outbox Pattern, Kafka producer
5. `InvoiceService.Api` — controller, CorrelationId middleware, Idempotency middleware
6. `InvoiceService.UnitTests` — testes do domínio puro
7. `InvoiceService.IntegrationTests` — Testcontainers (Kafka + Postgres)
