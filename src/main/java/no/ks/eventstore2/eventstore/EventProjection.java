package no.ks.eventstore2.eventstore;

import akka.actor.UntypedActor;
import akka.event.EventStream;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

public class EventProjection extends UntypedActor{
    List<Event> eventProjection = new ArrayList<Event>();

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            eventProjection.add((Event) o);
        } else if (o instanceof String){
            String message = (String) o;
            if (message.equals("getProjectedEvents")){
                getProjectedEvents();
            }
        }
    }

    private void getProjectedEvents() {
        sender().tell(ImmutableSet.copyOf(eventProjection), self());
    }

    @Override
    public void preStart(){
        EventStream eventStream = getContext().system().eventStream();
        eventStream.subscribe(self(), Event.class);
    }
}
