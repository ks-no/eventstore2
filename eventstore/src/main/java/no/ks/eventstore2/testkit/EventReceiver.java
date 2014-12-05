package no.ks.eventstore2.testkit;

import java.lang.Exception;import java.lang.Object;import java.lang.Override;import java.util.ArrayList;
import java.util.List;

import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.AcknowledgePreviousEventsProcessed;
import no.ks.eventstore2.response.Success;
import akka.actor.UntypedActor;

public class EventReceiver extends UntypedActor {
    public List<Event> receivedEvents = new ArrayList<Event>();

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof AcknowledgePreviousEventsProcessed) {
            sender().tell(new Success(), self());
        } else if (o instanceof Event) {
            receivedEvents.add((Event) o);
        }
    }
}