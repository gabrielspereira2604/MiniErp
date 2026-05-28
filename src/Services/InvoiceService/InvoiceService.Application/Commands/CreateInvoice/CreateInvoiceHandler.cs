using InvoiceService.Domain.Aggregates;
using InvoiceService.Domain.Ports;
using InvoiceService.Domain.ValueObjects;
using MediatR;

namespace InvoiceService.Application.Commands.CreateInvoice;

public class CreateInvoiceHandler : IRequestHandler<CreateInvoiceCommand, CreateInvoiceResult>
{
    private readonly IInvoiceRepository _repository;

    public CreateInvoiceHandler(IInvoiceRepository repository)
    {
        _repository = repository;
    }

    public async Task<CreateInvoiceResult> Handle(CreateInvoiceCommand command, CancellationToken cancellationToken)
    {
        var money = new Money(command.Amount, command.Currency);
        var invoice = Invoice.Create(command.IdempotencyKey, money, command.CustomerDocument);

        await _repository.SaveAsync(invoice, cancellationToken);

        return new CreateInvoiceResult(invoice.Id);
    }
}
