using FinancialService.Domain.Ports;
using MediatR;
using Shared.Kernel;

namespace FinancialService.Application.Commands.ReverseReceivable;

public class ReverseReceivableHandler : IRequestHandler<ReverseReceivableCommand>
{
    private readonly IReceivableRepository _repository;

    public ReverseReceivableHandler(IReceivableRepository repository)
    {
        _repository = repository;
    }

    public async Task Handle(ReverseReceivableCommand command, CancellationToken cancellationToken)
    {
        var receivable = await _repository.GetByInvoiceIdAsync(command.InvoiceId, cancellationToken)
            ?? throw new DomainException($"Receivable não encontrado para invoice {command.InvoiceId}");

        receivable.Reverse(command.Reason);

        await _repository.SaveAsync(receivable, cancellationToken);
    }
}
