package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.eventstore.LiveSubscription;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import org.junit.Test;

public class LiveSubscriptionProjectionTest extends Eventstore2TestKit{


    @Test
    public void testProjectionDoesNotGetOldEventsAfterSubscription() throws Exception {
        final ActorRef projection = _system.actorOf(Props.create(TestLiveSubscriptionProjection.class, super.testActor()));

        expectMsgClass(LiveSubscription.class);
    }

    @Test
    public void testProjectionGetsNewEvents() throws Exception {
        final ActorRef projection = _system.actorOf(Props.create(TestLiveSubscriptionProjection.class, super.testActor()));

        expectMsgClass(LiveSubscription.class);
        projection.tell(new CompleteSubscriptionRegistered("liveTest"), super.testActor());

        projection.tell(new FormParsed("fromid"), super.testActor());
        expectMsg("OK");
    }
}
