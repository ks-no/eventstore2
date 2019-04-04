package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.formProcessorProject.FormStatus;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.util.Failure;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static akka.pattern.Patterns.ask;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
class ProjectionCallTest extends EventstoreEventstore2TestKit {

    @Test
    void testProjectionReturnsStatusOnCallWithNoArgs() throws Exception {
        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, this.eventstoreConnection), "lastFormStatus1");
        int numberOfForms = (int)Await.result(ask(ref, call("getNumberOfForms"), 3000), Duration.create(3, TimeUnit.SECONDS));

        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), buildFormParsed()));

        callAssertEqualsWithRetry(ref, call("getNumberOfForms"),
                (Consumer<Object>) result -> assertThat(result, is(numberOfForms + 1)));
    }

    @Test
    void testProjectionReturnsStatusForSpecifiedFormOnCallWithArgs() {
        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, this.eventstoreConnection), "lastFormStatus3");

        String formId = UUID.randomUUID().toString();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(formId).build()));
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), buildFormReceived()));
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), EventStoreForm.FormParsed.newBuilder().setFormId(formId).build()));

        callAssertEqualsWithRetry(ref, call("getStatus", formId),
                result -> assertThat(result, is(FormStatus.PARSED)));
    }

    @Test
    void testProjectionMethodsAreCalledIfParametersAreAssignableToSuperclassOrInterface() {
        final TestActorRef<FormStatuses> ref = TestActorRef.create(_system, Props.create(FormStatuses.class, this.eventstoreConnection), "lastFormStatus2");

        String formId1 = UUID.randomUUID().toString();
        String formId2 = UUID.randomUUID().toString();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(formId1).build()));
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), EventStoreForm.FormReceived.newBuilder().setFormId(formId2).build()));
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), EventStoreForm.FormParsed.newBuilder().setFormId(formId1).build()));

        callAssertEqualsWithRetry(ref, call("getStatuses", Arrays.asList(formId1, formId2)),
                result -> assertThat(((Map<String, FormStatus>) result).size(), is(2)));
    }

    @Test
    void testErrorIsReceivedAtErrorListener() throws Exception {
        List<Props> props = new ArrayList<>();
        props.add(Props.create(FailingProjection.class, eventstoreConnection));
        final TestActorRef<ProjectionManager> ref = TestActorRef.create(_system, ProjectionManager.mkProps(super.testActor(), props), "failingProjection");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FailingProjection.class), 3000);
        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        EventStoreForm.FormReceived formReceived1 = buildFormReceived();
        EventStoreForm.FormReceived formReceived2 = buildFormReceived();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), formReceived1));
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), formReceived2));
        expectMsgClass(ProjectionFailedError.class);
        callAssertEqualsWithRetry(projectionRef, call("getFormById", formReceived2.getFormId()),
                result -> assertThat(result, is(formReceived2)));
    }

    @Test
    void testProjectionCallWithFutureMethod() throws Exception {
        TestActorRef<FutureCallProjection> projection = TestActorRef.create(_system, Props.create(FutureCallProjection.class, eventstoreConnection));
        final Future<Object> getString = ask(projection, call("getString"), 3000);
        final Object result = Await.result(getString, Duration.create(3, TimeUnit.SECONDS));
        assertThat(result, is("OK"));
    }

    @Test
    void testFailingFufutreCall() throws Exception {
        TestActorRef<FutureCallProjection> projection = TestActorRef.create(_system, Props.create(FutureCallProjection.class, eventstoreConnection));
        final Future<Object> getString = ask(projection, call("getFailure"), 3000);
        final Object result = Await.result(getString, Duration.create(3, TimeUnit.SECONDS));
        assertThat(((Failure)result).exception().getClass(), is(RuntimeException.class));

    }

    @Test
    void checkIntIntegerMethodCall() throws Exception {
        final Integer value = new Integer(10);
        TestActorRef<FutureCallProjection> projection = TestActorRef.create(_system, Props.create(FutureCallProjection.class, eventstoreConnection));
        final Future<Object> getString = ask(projection, call("getInt", value), 3000);
        final Object result = Await.result(getString, Duration.create(3, TimeUnit.SECONDS));
        assertThat(result, is(10));
    }

    private void callAssertEqualsWithRetry(ActorRef caller, Call call, Consumer... asserts) {
        new TestInvoker().invoke(() -> {
            try {
                Future<Object> future = ask(caller, call, 3000);
                Object result = Await.result(future, Duration.create(1, TimeUnit.SECONDS));
                Arrays.stream(asserts).forEach(a -> a.accept(result));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
