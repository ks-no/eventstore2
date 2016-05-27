package no.ks.eventstore2.eventstore;


import no.ks.eventstore2.KyroSerializable;

public class RemoveSubscription implements KyroSerializable {

    private String aggregateType;

    public RemoveSubscription(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }
}
