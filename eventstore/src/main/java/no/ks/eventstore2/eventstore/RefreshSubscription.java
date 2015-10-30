package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class RefreshSubscription implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String aggregateType;

	public RefreshSubscription(String aggregateType) {
		this.aggregateType = aggregateType;
	}

	public String getAggregateType() {
		return aggregateType;
	}

	@Override
	public String toString() {
		return "RefreshSubscription{"
				+"aggregateType='" + aggregateType + '\''
				+'}';
	}
}
