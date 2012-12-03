package no.ks.eventstore2.eventstore;

import java.io.Serializable;

public class RemoteSubscription implements Serializable {

	private String aggregateId;

	public RemoteSubscription(String aggregateId) {

		this.aggregateId = aggregateId;
	}

	public RemoteSubscription(Subscription s){
		this(s.getAggregateId());
	}

	public String getAggregateId() {
		return aggregateId;
	}
}
