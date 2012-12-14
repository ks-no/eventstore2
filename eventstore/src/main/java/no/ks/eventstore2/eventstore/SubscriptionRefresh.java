package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Set;

public class SubscriptionRefresh implements Serializable {
	private final String aggregateId;
	private final Set<ActorRef> subscribers;

	public SubscriptionRefresh(String aggregateId, Set<ActorRef> subscribers) {
		this.aggregateId = aggregateId;
		this.subscribers = subscribers;
	}

	public String getAggregateId() {
		return aggregateId;
	}

	public Set<ActorRef> getSubscribers() {
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
