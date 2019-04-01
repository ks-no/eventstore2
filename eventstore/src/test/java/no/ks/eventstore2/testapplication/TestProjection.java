package no.ks.eventstore2.testapplication;

import akka.actor.ActorRef;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ask.Asker;
import no.ks.eventstore2.projection.Projection;
import no.ks.eventstore2.projection.Subscriber;

import java.util.ArrayList;
import java.util.List;

@Subscriber("Test")
public class TestProjection extends Projection {

    private boolean isEventReceived = false;
    private List<EventstoreTest.TestEvent> events = new ArrayList<>();

    public TestProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Handler
    public void handleEvent(EventstoreTest.TestEvent event){
        isEventReceived = true;
        events.add(event);
    }

    public boolean isEventReceived() {
        return isEventReceived;
    }

    public List<EventstoreTest.TestEvent> getEvents() {
        return events;
    }

    public static boolean askIsEventReceived(ActorRef projection) {
        try {
            return Asker.askProjection(projection, "isEventReceived").single(Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("Error when asking projection " + e);
        }
    }

    public static List<EventstoreTest.TestEvent> askEvents(ActorRef projection) {
        try {
            return Asker.askProjection(projection, "getEvents").list(EventstoreTest.TestEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Error when asking projection " + e);
        }
    }
}