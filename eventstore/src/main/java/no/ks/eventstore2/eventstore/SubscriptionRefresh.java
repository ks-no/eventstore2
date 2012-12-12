package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class SubscriptionRefresh implements Serializable {
	private final String aggregateId;
	private final List<ActorRef> subscribers;

	public SubscriptionRefresh(String aggregateId, List<ActorRef> subscribers) {
		this.aggregateId = aggregateId;
		this.subscribers = subscribers;
	}

	public String getAggregateId() {
		return aggregateId;
	}

	public List<ActorRef> getSubscribers() {
		return subscribers;
	}

	@Override
	public String toString() {
		return "SubscriptionRefresh{"
				+"aggregateId='" + aggregateId + '\''
				+", subscribers=" + subscribers
				+'}';
	}
}
