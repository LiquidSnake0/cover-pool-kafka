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
        // This test documents the flaw, it does not fix it.
        //
        // Repaid carries a delta, not an absolute value. Kafka delivers at
        // least once, so this replay WILL happen. That is exactly why the
        // consumer deduplicates on EventId: without the guard, outstanding
        // principal is wrong, so the LTV is wrong, so eligibility is wrong,
        // so a wrong figure reaches the regulator.
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));

        var repayment = LoanEvent.Repaid("CH-0001", 50_000m, T.AddSeconds(1));
        loan.Apply(repayment);
        loan.Apply(repayment);   // the same event, redelivered

        Assert.Equal(300_000m, loan.OutstandingPrincipal);   // not 350,000
    }

    [Fact]
    public void Replaying_a_revaluation_is_harmless()
    {
        // The mirror image: Revalued carries an absolute value, so replaying it
        // has no effect. The general rule sits in this pair of tests — absolute
        // events are naturally idempotent, deltas are not.
        var loan = Fresh();
        loan.Apply(LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T));

        var revaluation = LoanEvent.Revalued("CH-0001", 450_000m, T.AddSeconds(1));
        loan.Apply(revaluation);
        loan.Apply(revaluation);

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
        // The heart of the project. The same three events in two orders give
        // two different eligibility histories — hence partitioning by LoanId.
        var origination = LoanEvent.Originated("CH-0001", 400_000m, 500_000m, T);
        var revaluation = LoanEvent.Revalued("CH-0001", 450_000m, T.AddSeconds(1));
        var repayment = LoanEvent.Repaid("CH-0001", 50_000m, T.AddSeconds(2));

        var inOrder = Fresh();
        foreach (var e in new[] { origination, revaluation, repayment })
            inOrder.Apply(e);

        var shuffled = Fresh();
        foreach (var e in new[] { origination, repayment, revaluation })
            shuffled.Apply(e);

        // The end states converge here, because the repayment is a delta and
        // the revaluation is absolute.
        Assert.Equal(inOrder.Ltv, shuffled.Ltv);

        // But the INTERMEDIATE state differs, and that is what drives entries
        // and exits from the pool. In order the loan leaves and comes back; out
        // of order it never leaves. The pool published at time t is not the same.
        var afterTwoInOrder = Fresh();
        afterTwoInOrder.Apply(origination);
        afterTwoInOrder.Apply(revaluation);
        Assert.False(EligibilityRules.Evaluate(afterTwoInOrder).Eligible);

        var afterTwoShuffled = Fresh();
        afterTwoShuffled.Apply(origination);
        afterTwoShuffled.Apply(repayment);
        Assert.True(EligibilityRules.Evaluate(afterTwoShuffled).Eligible);
    }
}
