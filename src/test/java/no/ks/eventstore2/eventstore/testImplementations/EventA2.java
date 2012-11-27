package no.ks.eventstore2.eventstore.testImplementations;

import no.ks.eventstore2.Event;

public class EventA2 extends Event {

    @Override
    public String getAggregateId(){
        return "A2";
    }
}
