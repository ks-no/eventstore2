package no.ks.eventstore2.eventstore;

import akka.actor.UntypedActor;
import akka.event.EventStream;

import java.util.ArrayList;
import java.util.List;

class EventStore extends UntypedActor {

    List<Event> store = new ArrayList<Event>();

    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            store.add((Event) o);
        } else if (o instanceof String){
            String message = (String) o;
            if (message.equals("publishEvents"))
                publishEvents();
        }
    }

    private void publishEvents() {
        EventStream eventStream = getContext().system().eventStream();
        for (Event event: store){
            eventStream.publish(event);
        }
    }

}
