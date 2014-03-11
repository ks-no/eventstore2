package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class CompleteSubscriptionRegistered implements Serializable{
    private String aggregateId;

    public CompleteSubscriptionRegistered(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }
}
