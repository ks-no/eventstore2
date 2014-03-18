package no.ks.eventstore2.eventstore;

public class UpgradeAggregate {
    private JournalStorage oldStorage;
    private String aggregateType;

    public UpgradeAggregate() {
    }

    public UpgradeAggregate(JournalStorage oldStorage, String aggregateType) {
        this.oldStorage = oldStorage;
        this.aggregateType = aggregateType;
    }

    public JournalStorage getOldStorage() {
        return oldStorage;
    }

    public void setOldStorage(JournalStorage oldStorage) {
        this.oldStorage = oldStorage;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }
}
