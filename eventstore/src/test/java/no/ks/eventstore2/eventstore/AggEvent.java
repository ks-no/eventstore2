package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

public class AggEvent extends Event {
	private static final long serialVersionUID = 1L;

	private String aggregateRootId;
	private String aggregateType;

	public AggEvent(String aggregateType) {
        this.aggregateRootId = "aggregaterootid";
		this.aggregateType = aggregateType;
	}

    public AggEvent(String aggregateRootId, String aggregateType) {
        this.aggregateRootId = aggregateRootId;
        this.aggregateType = aggregateType;
    }

    @Override
	public String getLogMessage() {
		return null;
	}

	@Override
	public String getAggregateRootId() {
		return aggregateRootId;
	}

	@Override
	public String getAggregateType() {
		return aggregateType;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggEvent)) return false;

        AggEvent aggEvent = (AggEvent) o;

        if (aggregateRootId != null ? !aggregateRootId.equals(aggEvent.aggregateRootId) : aggEvent.aggregateRootId != null)
            return false;
        if (aggregateType != null ? !aggregateType.equals(aggEvent.aggregateType) : aggEvent.aggregateType != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = aggregateRootId != null ? aggregateRootId.hashCode() : 0;
        result = 31 * result + (aggregateType != null ? aggregateType.hashCode() : 0);
        return result;
    }
}
