namespace CoverPool.Consumer;

/// <summary>
/// The rules deciding whether a loan may sit in the cover pool.
/// Deliberately simplified, the real ones fill tens of pages of a contractual
/// document.
/// </summary>
public static class EligibilityRules
{
    /// <summary>Usual LTV cap for residential assets in a covered bond pool.</summary>
    public const decimal MaxLtv = 0.80m;

    public const string EligibleCurrency = "CHF";

    public static (bool Eligible, string Reason) Evaluate(LoanState loan)
    {
        if (loan.InDefault)
            return (false, "in default");

        if (loan.Currency != EligibleCurrency)
            return (false, $"currency {loan.Currency}");

        if (loan.OutstandingPrincipal <= 0)
            return (false, "nothing outstanding");

        if (loan.PropertyValue <= 0)
            return (false, "no valuation");

        if (loan.Ltv > MaxLtv)
            return (false, $"LTV {loan.Ltv:P1} > {MaxLtv:P0}");

        return (true, $"LTV {loan.Ltv:P1}");
    }
}
