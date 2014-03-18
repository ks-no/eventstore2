package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;import java.lang.Override;import java.lang.String;

public class AggEvent extends Event {
	private static final long serialVersionUID = 1L;

	private String aggregateRootId;
	private String aggregateType;

	public AggEvent(String aggregateType) {
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
}
