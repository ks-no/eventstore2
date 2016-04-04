package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.KyroSerializable;

public class CompleteSubscriptionRegistered implements KyroSerializable {
	private static final long serialVersionUID = 1L;

	private String aggregateType;

	public CompleteSubscriptionRegistered(String aggregateType) {
		this.aggregateType = aggregateType;
	}

	public String getAggregateType() {
		return aggregateType;
	}

}
