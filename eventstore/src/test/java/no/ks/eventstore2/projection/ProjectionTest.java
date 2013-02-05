package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormReceived;
import no.ks.eventstore2.formProcessorProject.FormStatus;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }

            @Override
            public Class<? extends Projection> getProjectionClass() {
                return FormStatuses.class;
            }
        }), "lastFormStatus1");

        ref.tell(new FormReceived("1"), super.testActor());
        Future<Object> numberOfForms = ask(ref, call("getNumberOfForms"), 3000);

        assertEquals(1, ((Await.result(numberOfForms, duration("3 seconds")))));
    }

    @Test
    public void testProjectionReturnsStatusForSpecifiedFormOnCallWithArgs() throws Exception {

        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, new Props(new ProjectionFactory(super.testActor()){
            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }

            @Override
            public Class<? extends Projection> getProjectionClass() {
                return FormStatuses.class;
            }
        }), "lastFormStatus3");

        ref.tell(new FormReceived("2"), super.testActor());
        ref.tell(new FormReceived("3"), super.testActor());
        ref.tell(new FormParsed("2"), super.testActor());
        Future<Object> formStatus = ask(ref, call("getStatus", "2"), 3000);

        assertEquals(FormStatus.PARSED, ((Await.result(formStatus, duration("3 seconds")))));
    }

    @Test
    public void testProjectionMethodsAreCalledIfParametersAreAssignableToSuperclassOrInterface() throws Exception {

        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, new Props(new ProjectionFactory(super.testActor()){
            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }

            @Override
            public Class<? extends Projection> getProjectionClass() {
                return FormStatuses.class;
            }
        }), "lastFormStatus2");

        ref.tell(new FormReceived("2"), super.testActor());
        ref.tell(new FormReceived("3"), super.testActor());
        ref.tell(new FormParsed("2"), super.testActor());

        List<String> ids = new ArrayList<String>();
        ids.add("2");
        ids.add("3");
        Future<Object> formStatus = ask(ref, call("getStatuses", ids), 3000);
        Map <String, FormStatus> result = (Map<String, FormStatus>) Await.result(formStatus, duration("3 seconds"));
        assertEquals(2, result.size());
    }

    @Test
    public void testErrorIsReceivedAtErrorListener() throws Exception {
        ProjectionFactory projectionFactory = new ProjectionFactory(super.testActor()) {
            public Actor create() throws Exception {
                return new Projection(eventstore) {
                    boolean failed = false;

                    @Override
                    public void handleEvent(Event event) {
                        if (failed)
                            sender().tell(event, self());
                        else {
                            failed = true;
                            throw new RuntimeException("Failing");
                        }
                    }
                };
            }

            @Override
            public Class<? extends Projection> getProjectionClass() {
                return Projection.class;
            }
        };
        ArrayList<ProjectionFactory> factories = new ArrayList<ProjectionFactory>();
        factories.add(projectionFactory);
        final TestActorRef<ProjectionManager> ref = TestActorRef.create(_system, new Props(new ProjectionManagerFactory(factories,super.testActor())), "failingProjection");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", Projection.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        FormParsed formParsed = new FormParsed("formid");
        projectionRef.tell(formParsed,super.testActor());
        projectionRef.tell(formParsed,super.testActor());

        expectMsgClass(ProjectionFailedError.class);
        expectMsg(formParsed);
    }
}
