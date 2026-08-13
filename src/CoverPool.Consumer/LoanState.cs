using CoverPool.Contracts;

namespace CoverPool.Consumer;

/// <summary>
/// The current state of a loan, rebuilt by replaying its events.
/// This is a projection: nothing is stored, everything is derived from the log.
/// </summary>
public class LoanState
{
    public required string LoanId { get; init; }
    public decimal OutstandingPrincipal { get; set; }
    public decimal PropertyValue { get; set; }
    public string Currency { get; set; } = "CHF";
    public bool InDefault { get; set; }
    public bool IsEligible { get; set; }

    /// <summary>Loan-to-value: outstanding principal divided by property value.</summary>
    public decimal Ltv => PropertyValue <= 0 ? 0 : OutstandingPrincipal / PropertyValue;

    public void Apply(LoanEvent e)
    {
        switch (e.Type)
        {
            case LoanEventType.Originated:
                OutstandingPrincipal = e.Principal ?? 0;
                PropertyValue = e.PropertyValue ?? 0;
                Currency = e.Currency;
                break;

            case LoanEventType.Revalued:
                PropertyValue = e.PropertyValue ?? PropertyValue;
                break;

            case LoanEventType.Repaid:
                // A delta, not an absolute value. This is exactly what makes
                // deduplication necessary: applied twice, the repayment counts
                // twice and the LTV is wrong.
                OutstandingPrincipal -= e.RepaymentAmount ?? 0;
                break;

            case LoanEventType.Defaulted:
                InDefault = true;
                break;
        }
    }
}
