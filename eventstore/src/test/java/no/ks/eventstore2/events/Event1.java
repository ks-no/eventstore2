package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class Event1 extends Event {
	private static final long serialVersionUID = 1L;

	@Override
	public String getLogMessage() {
		return null;  
	}

	@Override
	public Event upgrade() {
		return new Event2();
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
