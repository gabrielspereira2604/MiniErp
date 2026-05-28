using FinancialService.Domain.Aggregates;
using FinancialService.Domain.Ports;
using FinancialService.Domain.ValueObjects;
using MediatR;

namespace FinancialService.Application.Commands.CreateReceivable;

public class CreateReceivableHandler : IRequestHandler<CreateReceivableCommand, CreateReceivableResult>
{
    private readonly IReceivableRepository _repository;

    public CreateReceivableHandler(IReceivableRepository repository)
    {
        _repository = repository;
    }

    public async Task<CreateReceivableResult> Handle(CreateReceivableCommand command, CancellationToken cancellationToken)
    {
        // idempotência — se já existe um receivable para essa invoice, retorna
        var existing = await _repository.GetByInvoiceIdAsync(command.InvoiceId, cancellationToken);
        if (existing is not null)
            return new CreateReceivableResult(existing.Id);

        var money = new Money(command.Amount, command.Currency);
        var receivable = Receivable.Create(Guid.NewGuid(), command.InvoiceId, money);

        await _repository.SaveAsync(receivable, cancellationToken);

        return new CreateReceivableResult(receivable.Id);
    }
}
