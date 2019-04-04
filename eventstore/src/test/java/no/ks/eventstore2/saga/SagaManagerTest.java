package no.ks.eventstore2.saga;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.google.common.cache.LoadingCache;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.formProcessorProject.DeliverForm;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.test.util.AssertionErrors.assertTrue;

@SuppressWarnings("unchecked")
class SagaManagerTest extends EventstoreEventstore2TestKit {

    private SagaInMemoryRepository sagaInMemoryRepository;

    private CommandDispatcher commandDispatcher;

    private TestInvoker invoker;

    SagaManagerTest() {
        this.invoker = new TestInvoker().withInterval(Duration.ofMillis(100));

        sagaInMemoryRepository = new SagaInMemoryRepository();
        ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchRequest.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchResult.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormReceived.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormParsed.getDefaultInstance());
    }

    @Test
    void testIncomingEventWithNewIdIsDispatchedToNewSaga() {
        TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, getSagaManagerProps(), "sagaManager_proto2");
        waitForLive(sagaManager.underlyingActor());
        int sizeBefore = commandDispatcher.received.size();

        EventStoreForm.FormReceived formReceived = EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", UUID.randomUUID().toString(), formReceived));
        invoker.invoke(() -> assertThat(commandDispatcher.received.get(sizeBefore).getClass(), is(ParseForm.class)));
        invoker.invoke(() -> assertThat(((ParseForm) commandDispatcher.received.get(sizeBefore)).getFormId(), is(formReceived.getFormId())));
    }

    @Test
    void testIncomingEventWithExistingIdIsDispatchedToExistingSaga() {
        TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, getSagaManagerProps(), "sagaManager1");
        waitForLive(sagaManager.underlyingActor());
        int sizeBefore = commandDispatcher.received.size();

        String aggregateRootId = UUID.randomUUID().toString();
        EventStoreForm.FormReceived formReceived = EventStoreForm.FormReceived.newBuilder().setFormId(UUID.randomUUID().toString()).build();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", aggregateRootId, formReceived));
        invoker.invoke(() -> assertThat(commandDispatcher.received.get(sizeBefore).getClass(), is(ParseForm.class)));
        invoker.invoke(() -> assertThat(((ParseForm) commandDispatcher.received.get(sizeBefore)).getFormId(), is(formReceived.getFormId())));
        EventStoreForm.FormParsed formParsed = EventStoreForm.FormParsed.newBuilder().setFormId(UUID.randomUUID().toString()).build();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", aggregateRootId, formParsed));
        invoker.invoke(() -> assertThat(commandDispatcher.received.get(sizeBefore + 1).getClass(), is(DeliverForm.class)));
        invoker.invoke(() -> assertThat(((DeliverForm) commandDispatcher.received.get(sizeBefore + 1)).getFormId(), is(formParsed.getFormId())));
    }

    @Test
    void testSagaStateSurvivesExceptionAndRestart() {
        final Props props = getSagaManagerProps();
        final TestActorRef<SagaManager> sagaManager = TestActorRef.create(_system, props, "sagaManager2");
        waitForLive(sagaManager.underlyingActor());
        int sizeBefore = commandDispatcher.received.size();

        String aggregateRootId = UUID.randomUUID().toString();
        EventStoreForm.FormReceived formReceived = EventStoreForm.FormReceived.getDefaultInstance();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", aggregateRootId, formReceived));
        invoker.invoke(() -> assertThat(commandDispatcher.received.get(sizeBefore).getClass(), is(ParseForm.class)));
        ((LoadingCache<SagaCompositeId, ActorRef>) ReflectionTestUtils.getField(sagaManager.underlyingActor(), "sagas")).invalidate(new SagaCompositeId(FormProcess.class, aggregateRootId));
        EventStoreForm.FormParsed formParsed = EventStoreForm.FormParsed.getDefaultInstance();
        journal.saveEvent(ProtobufHelper.newEventWrapper("form", aggregateRootId, formParsed));
        invoker.invoke(() -> assertThat(commandDispatcher.received.get(sizeBefore + 1).getClass(), is(DeliverForm.class)));

        assertThat(((ParseForm) commandDispatcher.received.get(sizeBefore)).getFormId(), is(formReceived.getFormId()));
        assertThat(((DeliverForm) commandDispatcher.received.get(sizeBefore + 1)).getFormId(), is(formParsed.getFormId()));
    }

    private Props getSagaManagerProps() {
        TestActorRef<CommandDispatcher> ref = TestActorRef.create(_system, Props.create(CommandDispatcher.class));
        this.commandDispatcher = ref.underlyingActor();
        return SagaManager.mkProps(_system, ref, sagaInMemoryRepository, eventstoreConnection, "no.ks.eventstore2.formProcessorProject");
    }

    void waitForLive(SagaManager manager) {
        invoker.invoke(() -> assertTrue("SagaManager is not live", manager.isLive()));
    }

    private static class CommandDispatcher extends AbstractActor {

        List<Object> received = new ArrayList<>();

        Object getLast() {
            return received.get(received.size() - 1);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(e -> received.add(e))
                    .build();
        }
    }
}
