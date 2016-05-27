package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.KyroSerializable;

public class SubscriptionRemoved implements KyroSerializable {
    private String aggregateType;

    public SubscriptionRemoved(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }
}
