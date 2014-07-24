package no.ks.eventstore2.eventstore;

public class AsyncSubscription extends Subscription {
    public AsyncSubscription(String aggregateType) {
        super(aggregateType);
    }

    public AsyncSubscription(String aggregateType, String fromJournalId) {
        super(aggregateType, fromJournalId);
    }

    @Override
    public String toString() {
        return "Asyncsubscription on '" + getAggregateType() + "' from '" + getFromJournalId() + "'";
    }
}
