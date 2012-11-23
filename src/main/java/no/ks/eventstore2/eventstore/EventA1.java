package no.ks.eventstore2.eventstore;

public class EventA1 extends Event {

    @Override
    public String getAggregateId(){
        return "A1";
    }
}
