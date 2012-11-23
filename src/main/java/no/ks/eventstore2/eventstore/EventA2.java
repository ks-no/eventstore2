package no.ks.eventstore2.eventstore;

public class EventA2 extends Event {

    @Override
    public String getAggregateId(){
        return "A2";
    }
}
