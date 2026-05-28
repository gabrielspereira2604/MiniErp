namespace Shared.Kernel;

public class DomainException : Exception
{
    public DomainException(string message) : base(message) { }
}
