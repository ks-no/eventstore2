package no.ks.eventstore2.eventstore;

public class UpgradeAggregate {
    private JournalStorage oldStorage;
    private String aggregateId;

    public UpgradeAggregate() {
    }

    public UpgradeAggregate(JournalStorage oldStorage, String aggregateId) {
        this.oldStorage = oldStorage;
        this.aggregateId = aggregateId;
    }

    public JournalStorage getOldStorage() {
        return oldStorage;
    }

    public void setOldStorage(JournalStorage oldStorage) {
        this.oldStorage = oldStorage;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }
}
