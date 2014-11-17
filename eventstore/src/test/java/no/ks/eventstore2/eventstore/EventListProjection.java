package no.ks.eventstore2.eventstore;

import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;

import java.util.ArrayList;
import java.util.List;


public class EventListProjection extends UntypedActor {

    private List<Event> events = new ArrayList<>();

    @Override
    public void onReceive(Object o) throws Exception {
        if(o instanceof Event){
            events.add((Event) o);
        }
    }

    public List<Event> getEvents(){
        return events;
    }
}
