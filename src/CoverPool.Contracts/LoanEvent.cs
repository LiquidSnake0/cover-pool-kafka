namespace CoverPool.Contracts;

/// <summary>
/// What travels on the topic. One envelope for every kind of loan event,
/// because they have to stay ordered relative to each other: a revaluation
/// followed by a repayment does not give the same result as the reverse.
/// </summary>
public record LoanEvent(
    string EventId,
    string LoanId,
    LoanEventType Type,
    DateTimeOffset OccurredAt,
    decimal? Principal = null,        // Originated
    decimal? PropertyValue = null,    // Originated, Revalued
    decimal? RepaymentAmount = null,  // Repaid, a delta, which is why dedup matters
    string Currency = "CHF")
{
    public static LoanEvent Originated(string loanId, decimal principal, decimal propertyValue,
                                       DateTimeOffset at, string currency = "CHF") =>
        new(NewId(), loanId, LoanEventType.Originated, at,
            Principal: principal, PropertyValue: propertyValue, Currency: currency);

    public static LoanEvent Revalued(string loanId, decimal propertyValue, DateTimeOffset at) =>
        new(NewId(), loanId, LoanEventType.Revalued, at, PropertyValue: propertyValue);

    public static LoanEvent Repaid(string loanId, decimal amount, DateTimeOffset at) =>
        new(NewId(), loanId, LoanEventType.Repaid, at, RepaymentAmount: amount);

    public static LoanEvent Defaulted(string loanId, DateTimeOffset at) =>
        new(NewId(), loanId, LoanEventType.Defaulted, at);

    private static string NewId() => Guid.NewGuid().ToString("N")[..12];
}

public enum LoanEventType
{
    /// <summary>The loan is granted: principal and property valuation.</summary>
    Originated,

    /// <summary>The property is revalued. LTV moves without the debt changing.</summary>
    Revalued,

    /// <summary>Partial repayment. Outstanding principal falls, LTV improves.</summary>
    Repaid,

    /// <summary>Payment default. Immediate exit from the cover pool.</summary>
    Defaulted,
}
