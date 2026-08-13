using CoverPool.Consumer;

namespace CoverPool.Tests;

public class EligibilityRulesTests
{
    private static LoanState Loan(decimal principal, decimal value,
                                  string currency = "CHF", bool inDefault = false) =>
        new()
        {
            LoanId = "CH-TEST",
            OutstandingPrincipal = principal,
            PropertyValue = value,
            Currency = currency,
            InDefault = inDefault,
        };

    [Fact]
    public void Comfortable_ltv_is_eligible()
    {
        var (eligible, _) = EligibilityRules.Evaluate(Loan(300_000m, 600_000m));
        Assert.True(eligible);
    }

    [Fact]
    public void Ltv_exactly_at_the_cap_is_eligible()
    {
        // La borne est incluse. Un « > » et un « >= » ne donnent pas le même
        // pool, et sur un portefeuille entier l'écart se chiffre en millions.
        var (eligible, _) = EligibilityRules.Evaluate(Loan(400_000m, 500_000m)); // 80,0 %
        Assert.True(eligible);
    }

    [Fact]
    public void Ltv_just_above_the_cap_is_not()
    {
        var (eligible, raison) = EligibilityRules.Evaluate(Loan(400_100m, 500_000m)); // 80,02 %
        Assert.False(eligible);
        Assert.Contains("LTV", raison);
    }

    [Fact]
    public void Default_excludes_regardless_of_ltv()
    {
        var (eligible, raison) = EligibilityRules.Evaluate(
            Loan(100_000m, 1_000_000m, inDefault: true)); // LTV 10 %
        Assert.False(eligible);
        Assert.Contains("défaut", raison);
    }

    [Fact]
    public void Foreign_currency_excludes()
    {
        var (eligible, raison) = EligibilityRules.Evaluate(Loan(200_000m, 400_000m, "EUR"));
        Assert.False(eligible);
        Assert.Contains("EUR", raison);
    }

    [Fact]
    public void Fully_repaid_loan_leaves_the_pool()
    {
        // Plus de capital restant, plus rien à nantir.
        var (eligible, _) = EligibilityRules.Evaluate(Loan(0m, 500_000m));
        Assert.False(eligible);
    }

    [Fact]
    public void Loan_without_a_valuation_is_not_eligible()
    {
        // Sans valorisation, pas de LTV calculable. Refuser plutôt que
        // supposer : une division par zéro qui rendrait 0 ferait entrer le
        // prêt avec un LTV apparent de 0 %.
        var (eligible, _) = EligibilityRules.Evaluate(Loan(300_000m, 0m));
        Assert.False(eligible);
    }
}
