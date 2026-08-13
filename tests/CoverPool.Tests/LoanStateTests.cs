using CoverPool.Consumer;
using CoverPool.Contracts;

namespace CoverPool.Tests;

public class LoanStateTests
{
    private static readonly DateTimeOffset T = new(2026, 8, 15, 9, 0, 0, TimeSpan.Zero);

    private static LoanState Fresh() => new() { LoanId = "CH-0001" };

    [Fact]
    public void Origination_sets_principal_and_valuation()
    {
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));

        Assert.Equal(400_000m, loan.OutstandingPrincipal);
        Assert.Equal(500_000m, loan.PropertyValue);
        Assert.Equal(0.80m, loan.Ltv);
    }

    [Fact]
    public void Revaluation_moves_ltv_without_touching_the_debt()
    {
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));
        loan.Apply(LoanEvent.Revalued("CH-0001", 450_000m, T.AddSeconds(1)));

        Assert.Equal(400_000m, loan.OutstandingPrincipal);
        Assert.True(loan.Ltv > 0.88m && loan.Ltv < 0.89m);
    }

    [Fact]
    public void Repayment_reduces_the_debt()
    {
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));
        loan.Apply(LoanEvent.Repaid("CH-0001", 50_000m, T.AddSeconds(1)));

        Assert.Equal(350_000m, loan.OutstandingPrincipal);
    }

    [Fact]
    public void Replaying_a_repayment_subtracts_twice()
    {
        // Ce test documente le défaut, il ne le corrige pas.
        //
        // Repaid porte un delta, pas une valeur absolue. Kafka livre « au moins
        // une fois », donc ce rejeu ARRIVERA. C'est exactement pour ça que le
        // consommateur déduplique sur EventId : sans ce garde-fou, le capital
        // restant est faux, donc le LTV est faux, donc l'éligibilité est fausse,
        // donc un chiffre faux part au régulateur.
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));

        var remboursement = LoanEvent.Repaid("CH-0001", 50_000m, T.AddSeconds(1));
        loan.Apply(remboursement);
        loan.Apply(remboursement);   // le même événement, relivré

        Assert.Equal(300_000m, loan.OutstandingPrincipal);   // et non 350 000
    }

    [Fact]
    public void Replaying_a_revaluation_is_harmless()
    {
        // À l'inverse : Revalued porte une valeur absolue, donc le rejeu est
        // sans effet. La règle générale — des événements absolus sont
        // naturellement idempotents, des deltas ne le sont pas.
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));

        var reevaluation = LoanEvent.Revalued("CH-0001", 450_000m, T.AddSeconds(1));
        loan.Apply(reevaluation);
        loan.Apply(reevaluation);

        Assert.Equal(450_000m, loan.PropertyValue);
    }

    [Fact]
    public void Default_is_recorded()
    {
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));
        loan.Apply(LoanEvent.Defaulted("CH-0001", T.AddSeconds(1)));

        Assert.True(loan.InDefault);
    }

    [Fact]
    public void Order_changes_the_outcome()
    {
        // Le cœur du projet. Les mêmes trois événements, deux ordres, deux
        // éligibilités différentes — d'où le partitionnement par LoanId.
        var origination = LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T);
        var reevaluation = LoanEvent.Revalued("CH-0001", 450_000m, T.AddSeconds(1));
        var remboursement = LoanEvent.Repaid("CH-0001", 50_000m, T.AddSeconds(2));

        var chronologique = Fresh();
        foreach (var e in new[] { origination, reevaluation, remboursement })
            chronologique.Apply(e);

        var desordonne = Fresh();
        foreach (var e in new[] { origination, remboursement, reevaluation })
            desordonne.Apply(e);

        // Ici les deux convergent, parce que le remboursement est un delta et
        // la réévaluation une valeur absolue.
        Assert.Equal(chronologique.Ltv, desordonne.Ltv);

        // Mais l'ÉTAT INTERMÉDIAIRE diffère, et c'est lui qui déclenche les
        // entrées et sorties du pool. Dans l'ordre, le prêt sort puis revient ;
        // dans le désordre, il ne sort jamais. Le pool publié à l'instant t
        // n'est pas le même.
        var apresDeuxEvenements = Fresh();
        apresDeuxEvenements.Apply(origination);
        apresDeuxEvenements.Apply(reevaluation);
        Assert.False(EligibilityRules.Evaluate(apresDeuxEvenements).Eligible);

        var apresDeuxEvenementsDesordre = Fresh();
        apresDeuxEvenementsDesordre.Apply(origination);
        apresDeuxEvenementsDesordre.Apply(remboursement);
        Assert.True(EligibilityRules.Evaluate(apresDeuxEvenementsDesordre).Eligible);
    }
}
