using CoverPool.Contracts;

namespace CoverPool.Consumer;

/// <summary>
/// L'état courant d'un prêt, reconstruit en rejouant ses événements.
/// C'est une projection : rien n'est stocké, tout est dérivé du journal.
/// </summary>
public class LoanState
{
    public required string LoanId { get; init; }
    public decimal OutstandingPrincipal { get; set; }
    public decimal PropertyValue { get; set; }
    public string Currency { get; set; } = "CHF";
    public bool InDefault { get; set; }
    public bool IsEligible { get; set; }

    /// <summary>Quotité de financement : capital restant ÷ valeur du bien.</summary>
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
                // Un delta, pas une valeur absolue. C'est précisément ce qui
                // rend la déduplication indispensable : appliqué deux fois,
                // le remboursement compte double et le LTV devient faux.
                OutstandingPrincipal -= e.RepaymentAmount ?? 0;
                break;

            case LoanEventType.Defaulted:
                InDefault = true;
                break;
        }
    }
}
