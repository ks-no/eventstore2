package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class NewEvent extends Event {
	private static final long serialVersionUID = 1L;

	@Override
	public String getLogMessage() {
		return null;
	}

	@Override
	public String getAggregateRootId() {
		return null;
	}

	@Override
	public String getAggregateType() {
		return "upgrade";
	}
}
