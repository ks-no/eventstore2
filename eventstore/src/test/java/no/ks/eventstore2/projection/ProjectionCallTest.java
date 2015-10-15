package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.eventstore.IncompleteSubscriptionPleaseSendNew;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormReceived;
import no.ks.eventstore2.formProcessorProject.FormStatus;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.util.Failure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;
import static akka.testkit.JavaTestKit.duration;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.Assert.assertEquals;

public class ProjectionCallTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public ProjectionCallTest() {
        super(_system);
    }

    @Test
    public void testProjectionReturnsStatusOnCallWithNoArgs() throws Exception {
        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, super.testActor()), "lastFormStatus1");

        ref.tell(new FormReceived("1"), super.testActor());
        ref.tell(new CompleteSubscriptionRegistered("agg"), super.testActor());
        Future<Object> numberOfForms = ask(ref, call("getNumberOfForms"), 3000);

        assertEquals(1, ((Await.result(numberOfForms, duration("3 seconds")))));
    }

    @Test
    public void testProjectionReturnsStatusForSpecifiedFormOnCallWithArgs() throws Exception {

        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, super.testActor()), "lastFormStatus3");

        ref.tell(new FormReceived("2"), super.testActor());
        ref.tell(new FormReceived("3"), super.testActor());
        ref.tell(new FormParsed("2"), super.testActor());
        ref.tell(new CompleteSubscriptionRegistered("agg"), super.testActor());
        Future<Object> formStatus = ask(ref, call("getStatus", "2"), 3000);

        assertEquals(FormStatus.PARSED, ((Await.result(formStatus, duration("3 seconds")))));
    }

    @Test
    public void testProjectionMethodsAreCalledIfParametersAreAssignableToSuperclassOrInterface() throws Exception {

        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, super.testActor()), "lastFormStatus2");

        ref.tell(new FormReceived("2"), super.testActor());
        ref.tell(new FormReceived("3"), super.testActor());
        ref.tell(new FormParsed("2"), super.testActor());
        ref.tell(new CompleteSubscriptionRegistered("agg"), super.testActor());
        List<String> ids = new ArrayList<String>();
        ids.add("2");
        ids.add("3");
        Future<Object> formStatus = ask(ref, call("getStatuses", ids), 3000);
        Map<String, FormStatus> result = (Map<String, FormStatus>) Await.result(formStatus, duration("3 seconds"));
        assertEquals(2, result.size());
    }

    @Test
    public void testErrorIsReceivedAtErrorListener() throws Exception {

        ArrayList<Props> props = new ArrayList<>();
        props.add(Props.create(FailingProjection.class, super.testActor()));
        final TestActorRef<ProjectionManager> ref = TestActorRef.create(_system, ProjectionManager.mkProps(super.testActor(), props), "failingProjection");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FailingProjection.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        FormParsed formParsed = new FormParsed("formid");
        projectionRef.tell(formParsed,super.testActor());
        projectionRef.tell(formParsed,super.testActor());
        expectMsgClass(Subscription.class);
        expectMsgClass(ProjectionFailedError.class);
        expectMsg(formParsed);
    }

    @Test
    public void testGetNewSubscription() throws Exception {
        TestActorRef<Actor> newSubscription = TestActorRef.create(_system, Props.create(FormStatuses.class, super.testActor()), "newSubscription");
        expectMsgClass(Subscription.class);
        FormParsed formid = new FormParsed("formid");
        formid.setJournalid("01");
        newSubscription.tell(formid,super.testActor());
        newSubscription.tell(new IncompleteSubscriptionPleaseSendNew("agg"),super.testActor());
        expectMsg(new Subscription("agg","01"));
    }

    @Test
    public void testPendingCallsAreFilled() throws Exception {
        TestActorRef<Actor> projection = TestActorRef.create(_system, Props.create(FormStatuses.class, super.testActor()), "pendingCalls");
        expectMsgClass(Subscription.class);
        FormParsed formid = new FormParsed("formid");
        formid.setJournalid("01");
        projection.tell(formid, super.testActor());
        Future<Object> nrStatusesFuture = ask(projection, call("getNumberOfForms"), 3000);
        projection.tell(new CompleteSubscriptionRegistered("agg"),super.testActor());
        Integer nrStatuser = (Integer) Await.result(nrStatusesFuture, Duration.create("3 seconds"));
        assertEquals(new Integer(1), nrStatuser);

    }

    @Test
    public void testProjectionCallWithFutureMethod() throws Exception {
        TestActorRef<Actor> projection = TestActorRef.create(_system, Props.create(FutureCallProjection.class, super.testActor()));
        expectMsgClass(Subscription.class);
        projection.tell(new CompleteSubscriptionRegistered("agg"), ActorRef.noSender());
        final Future<Object> getString = ask(projection, call("getString"), 3000);
        final Object result = Await.result(getString, Duration.create(3, TimeUnit.SECONDS));
        assertEquals("OK", result);
    }

    @Test
    public void testFailingFufutreCall() throws Exception {
        TestActorRef<Actor> projection = TestActorRef.create(_system, Props.create(FutureCallProjection.class, super.testActor()));
        expectMsgClass(Subscription.class);
        projection.tell(new CompleteSubscriptionRegistered("agg"), ActorRef.noSender());
        final Future<Object> getString = ask(projection, call("getFailure"), 3000);
        final Object result = Await.result(getString, Duration.create(3, TimeUnit.SECONDS));
        assertEquals(RuntimeException.class, ((Failure)result).exception().getClass());

    }
}
