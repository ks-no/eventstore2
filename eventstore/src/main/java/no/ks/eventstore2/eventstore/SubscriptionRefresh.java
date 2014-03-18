package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class SubscriptionRefresh implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String aggregateType;
	private final Set<ActorRef> subscribers;

	public SubscriptionRefresh(String aggregateType, Set<ActorRef> subscribers) {
		this.aggregateType = aggregateType;
		this.subscribers = new HashSet<ActorRef>(subscribers);
	}

	public String getAggregateType() {
		return aggregateType;
	}

	public Set<ActorRef> getSubscribers() {
		return subscribers;
	}

	@Override
	public String toString() {
		return "SubscriptionRefresh{"
				+"aggregateType='" + aggregateType + '\''
				+", subscribers=" + subscribers
				+'}';
	}
}
