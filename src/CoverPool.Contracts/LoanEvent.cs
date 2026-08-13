namespace CoverPool.Contracts;

/// <summary>
/// Ce qui circule sur le topic. Un seul type d'enveloppe pour tous les
/// événements d'un prêt, parce qu'ils doivent rester ordonnés entre eux :
/// une réévaluation suivie d'un remboursement ne donne pas le même résultat
/// dans l'autre sens.
/// </summary>
public record LoanEvent(
    string EventId,
    string LoanId,
    LoanEventType Type,
    DateTimeOffset OccurredAt,
    decimal? Principal = null,        // Originated
    decimal? PropertyValue = null,    // Originated, Revalued
    decimal? RepaymentAmount = null,  // Repaid  — un delta, d'où le besoin de déduplication
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
    /// <summary>Le crédit est accordé : montant et valeur du bien.</summary>
    Originated,

    /// <summary>Le bien est réévalué. Le LTV change sans que le prêt bouge.</summary>
    Revalued,

    /// <summary>Remboursement partiel. Le capital restant baisse, le LTV s'améliore.</summary>
    Repaid,

    /// <summary>Défaut de paiement. Sortie immédiate du cover pool.</summary>
    Defaulted,
}
