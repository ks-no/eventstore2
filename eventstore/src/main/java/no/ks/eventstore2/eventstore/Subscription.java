package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class Subscription implements Serializable{

	private String aggregateId;
    private String fromJournalId;

    /**
     * Subscription from start
     * @param aggregateId
     */
    public Subscription(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    /**
     * Subscription from last event
     * @param aggregateId
     * @param fromJournalId lastJournalidReceived (Not the next one to expect)
     */
    public Subscription(String aggregateId, String fromJournalId) {
        this.aggregateId = aggregateId;
        this.fromJournalId = fromJournalId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getFromJournalId() {
        return fromJournalId;
    }

    @Override
    public String toString() {
        return "Subscription on '" + aggregateId + "' from '" + fromJournalId + "'";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription)) return false;

        Subscription that = (Subscription) o;

        if (aggregateId != null ? !aggregateId.equals(that.aggregateId) : that.aggregateId != null) return false;
        if (fromJournalId != null ? !fromJournalId.equals(that.fromJournalId) : that.fromJournalId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = aggregateId != null ? aggregateId.hashCode() : 0;
        result = 31 * result + (fromJournalId != null ? fromJournalId.hashCode() : 0);
        return result;
    }
}
