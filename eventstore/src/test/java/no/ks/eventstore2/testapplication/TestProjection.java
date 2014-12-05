package no.ks.eventstore2.testapplication;

import akka.actor.ActorRef;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ask.Asker;
import no.ks.eventstore2.projection.Projection;
import no.ks.eventstore2.projection.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Subscriber(AggregateType.TEST_AGGREGATE)
public class TestProjection extends Projection {

    private static Logger log = LoggerFactory.getLogger(TestProjection.class);

    private boolean isEventReceived = false;
    private List<TestEvent> events = new ArrayList<>();

    public TestProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Handler
    public void handleEvent(TestEvent event){
        isEventReceived = true;
        events.add(event);
    }

    public boolean isEventReceived() {
        return isEventReceived;
    }

    public List<TestEvent> getEvents(String arg1, String arg2) {
        return events;
    }

    public static boolean askIsEventReceived(ActorRef projection) {
        try {
            return Asker.askProjection(projection, "isEventReceived").single(Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("Error when asking projection " + e);
        }
    }

    public static List<TestEvent> askEvents(ActorRef projection, String arg1, String arg2) {
        try {
            return Asker.askProjection(projection, "getEvents", arg1, arg2).list(TestEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Error when asking projection " + e);
        }
    }
}