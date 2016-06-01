package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.KyroSerializable;

public class RetrieveAggregateEventsAsync implements KyroSerializable {
    private String aggregateId;
    private String fromJournalId;
    private String aggregateType;

    /**
     *
     * @param aggregateType
     * @param aggregateId
     * @param fromJournalId null if read from start
     */
    public RetrieveAggregateEventsAsync(String aggregateType, String aggregateId, String fromJournalId) {
        this.aggregateId = aggregateId;
        this.fromJournalId = fromJournalId;
        this.aggregateType = aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getFromJournalId() {
        return fromJournalId;
    }

    public String getAggregateType() {
        return aggregateType;
    }


}
