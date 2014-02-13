package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class IncompleteSubscriptionPleaseSendNew implements Serializable {

    private String aggregateId;

    public IncompleteSubscriptionPleaseSendNew(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IncompleteSubscriptionPleaseSendNew that = (IncompleteSubscriptionPleaseSendNew) o;

        if (aggregateId != null ? !aggregateId.equals(that.aggregateId) : that.aggregateId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return aggregateId != null ? aggregateId.hashCode() : 0;
    }
}
