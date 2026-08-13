namespace CoverPool.Consumer;

/// <summary>
/// Les règles qui décident si un prêt peut figurer dans le cover pool.
/// Volontairement simplifiées — les vraies tiennent dans un document
/// contractuel de plusieurs dizaines de pages.
/// </summary>
public static class EligibilityRules
{
    /// <summary>Plafond de quotité usuel pour du résidentiel en covered bond.</summary>
    public const decimal MaxLtv = 0.80m;

    public const string EligibleCurrency = "CHF";

    public static (bool Eligible, string Reason) Evaluate(LoanState loan)
    {
        if (loan.InDefault)
            return (false, "en défaut");

        if (loan.Currency != EligibleCurrency)
            return (false, $"devise {loan.Currency}");

        if (loan.OutstandingPrincipal <= 0)
            return (false, "capital restant nul");

        if (loan.PropertyValue <= 0)
            return (false, "pas de valorisation");

        if (loan.Ltv > MaxLtv)
            return (false, $"LTV {loan.Ltv:P1} > {MaxLtv:P0}");

        return (true, $"LTV {loan.Ltv:P1}");
    }
}
