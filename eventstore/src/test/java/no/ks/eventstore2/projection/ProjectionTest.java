package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormReceived;
import no.ks.eventstore2.formProcessorProject.FormStatus;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;

import static akka.pattern.Patterns.ask;
import static akka.testkit.JavaTestKit.duration;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.Assert.assertEquals;

public class ProjectionTest extends TestKit{

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public ProjectionTest() {
        super(_system);
    }

    @Test
    public void testProjectionReturnsStatusOnCallWithNoArgs() throws Exception {
        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, new Props(new ProjectionFactory(super.testActor()){
            @Override
            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }
        }), "lastFormStatus1");

        ref.tell(new FormReceived("1"), super.testActor());
        Future<Object> numberOfForms = ask(ref, call("getNumberOfForms"), 3000);

        assertEquals(1, ((Await.result(numberOfForms, duration("3 seconds")))));
    }

    @Test
    public void testProjectionReturnsStatusForSpecifiedFormOnCallWithArgs() throws Exception {

        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, new Props(new ProjectionFactory(super.testActor()){
            @Override
            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }
        }), "lastFormStatus2");

        ref.tell(new FormReceived("2"), super.testActor());
        ref.tell(new FormReceived("3"), super.testActor());
        ref.tell(new FormParsed("2"), super.testActor());
        Future<Object> formStatus = ask(ref, call("getStatus", "2"), 3000);

        assertEquals(FormStatus.PARSED, ((Await.result(formStatus, duration("3 seconds")))));
    }
}
