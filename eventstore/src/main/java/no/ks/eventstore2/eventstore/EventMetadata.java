package no.ks.eventstore2.eventstore;

public class EventMetadata {
    private String aggregateType;
    private String aggregateRootId;
    private long occuredOn;
    private String protoSerializationType;
    private String createdByUser;
    private long journalId;
    private String correlationId;

    EventMetadata(String aggregateType, String aggregateRootId, long occuredOn, String protoSerializationType,
                         String createdByUser, long journalId, String correlationId) {
        this.aggregateType = aggregateType;
        this.aggregateRootId = aggregateRootId;
        this.occuredOn = occuredOn;
        this.protoSerializationType = protoSerializationType;
        this.createdByUser = createdByUser;
        this.journalId = journalId;
        this.correlationId = correlationId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateRootId() {
        return aggregateRootId;
    }

    public long getOccuredOn() {
        return occuredOn;
    }

    public String getProtoSerializationType() {
        return protoSerializationType;
    }

    public String getCreatedByUser() {
        return createdByUser;
    }

    public long getJournalId() {
        return journalId;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
