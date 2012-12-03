package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class Subscription implements Serializable{
    private String aggregateId;

    public Subscription(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateId() {
        return aggregateId;
    }
}
