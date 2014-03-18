package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class Event2 extends Event {
	private static final long serialVersionUID = 1L;

	@Override
	public String getLogMessage() {
		return null;
	}

	@Override
	public Event upgrade() {
		return new Event3();
	}

	@Override
	public String getAggregateRootId() {
		return "1";
	}

	@Override
	public String getAggregateType() {
		return "upgrade";
	}
}
