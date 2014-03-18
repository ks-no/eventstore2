package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class CompleteSubscriptionRegistered implements Serializable{
	private static final long serialVersionUID = 1L;

	private String aggregateType;

	public CompleteSubscriptionRegistered(String aggregateType) {
		this.aggregateType = aggregateType;
	}

	public String getAggregateType() {
		return aggregateType;
	}

}
