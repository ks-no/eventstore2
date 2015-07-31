package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.formProcessorProject.FormParsed;

@Subscriber("liveTest")
public class TestLiveSubscriptionProjection extends LiveSubscriptionProjection {

    public TestLiveSubscriptionProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Handler
    public void handleEvent(FormParsed event) {
        sender().tell("OK", self());
    }
}
