package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;import java.lang.Override;import java.lang.String;

public class AggEvent extends Event {

    public AggEvent(String aggregate) {
        setAggregateId(aggregate);
    }

    @Override
    public String getLogMessage() {
        return null;
    }
}
