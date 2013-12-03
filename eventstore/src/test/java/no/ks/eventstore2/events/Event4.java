package no.ks.eventstore2.events;

import no.ks.eventstore2.Event;

public class Event4 extends Event {

    public Event4() {
        setAggregateId("upgrade");
    }


    @Override
    public String getLogMessage() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
