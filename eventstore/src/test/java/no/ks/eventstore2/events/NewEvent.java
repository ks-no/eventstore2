package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class NewEvent extends Event {

    public NewEvent() {
        setAggregateId("upgrade");
    }

    @Override
    public String getLogMessage() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
