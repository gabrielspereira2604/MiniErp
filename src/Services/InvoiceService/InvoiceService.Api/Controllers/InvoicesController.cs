using InvoiceService.Application.Commands.CreateInvoice;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace InvoiceService.Api.Controllers;

[ApiController]
[Route("invoices")]
public class InvoicesController : ControllerBase
{
    private readonly IMediator _mediator;

    public InvoicesController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost]
    public async Task<IActionResult> Create(
        [FromBody] CreateInvoiceRequest request,
        CancellationToken cancellationToken)
    {
        var command = new CreateInvoiceCommand(
            request.IdempotencyKey,
            request.Amount,
            request.Currency,
            request.CustomerDocument
        );

        var result = await _mediator.Send(command, cancellationToken);

        return CreatedAtAction(nameof(GetById), new { id = result.InvoiceId }, result);
    }

    [HttpGet("{id:guid}")]
    public IActionResult GetById(Guid id)
    {
        return Ok(new { id });
    }
}

public record CreateInvoiceRequest(
    Guid IdempotencyKey,
    decimal Amount,
    string Currency,
    string CustomerDocument
);
