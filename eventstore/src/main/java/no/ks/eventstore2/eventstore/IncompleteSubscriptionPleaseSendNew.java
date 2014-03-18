package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class IncompleteSubscriptionPleaseSendNew implements Serializable {
	private static final long serialVersionUID = 1L;

	private String aggregateType;

    public IncompleteSubscriptionPleaseSendNew(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IncompleteSubscriptionPleaseSendNew that = (IncompleteSubscriptionPleaseSendNew) o;

        if (aggregateType != null ? !aggregateType.equals(that.aggregateType) : that.aggregateType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return aggregateType != null ? aggregateType.hashCode() : 0;
    }
}
