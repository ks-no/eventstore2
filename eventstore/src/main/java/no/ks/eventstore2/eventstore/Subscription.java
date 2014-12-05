package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class Subscription implements Serializable{
	private static final long serialVersionUID = 1L;

	private String aggregateType;
    private String fromJournalId;

    /**
     * Subscription from start
     * @param aggregateType
     */
    public Subscription(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    /**
     * Subscription from last event
     * @param aggregateType
     * @param fromJournalId lastJournalidReceived (Not the next one to expect)
     */
    public Subscription(String aggregateType, String fromJournalId) {
        this.aggregateType = aggregateType;
        this.fromJournalId = fromJournalId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getFromJournalId() {
        return fromJournalId;
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "aggregateType='" + aggregateType + '\'' +
                ", fromJournalId='" + fromJournalId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription)) return false;

        Subscription that = (Subscription) o;

        if (aggregateType != null ? !aggregateType.equals(that.aggregateType) : that.aggregateType != null) return false;
        if (fromJournalId != null ? !fromJournalId.equals(that.fromJournalId) : that.fromJournalId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = aggregateType != null ? aggregateType.hashCode() : 0;
        result = 31 * result + (fromJournalId != null ? fromJournalId.hashCode() : 0);
        return result;
    }
}
