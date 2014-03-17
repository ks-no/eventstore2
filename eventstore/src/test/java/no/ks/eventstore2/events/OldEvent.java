package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class OldEvent extends Event {

    public OldEvent() {
        setAggregateId("upgrade");
    }

    @Override
    public String getLogMessage() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Event upgrade() {
        return new NewEvent();
    }

    @Override
    public String getAggregateRootId() {
        return "1";
    }
}
