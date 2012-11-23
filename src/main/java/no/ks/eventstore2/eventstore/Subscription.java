package no.ks.eventstore2.eventstore;

public class Subscription {
    private String aggregateId;

    public Subscription(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

}
