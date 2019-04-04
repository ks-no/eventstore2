package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProjectionTest extends EventstoreEventstore2TestKit {

    @Test
    void testProjectionCanReceiveEventsAndAnswerCall() {
        TestActorRef<TestProjection> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, this.eventstoreConnection), UUID.randomUUID().toString());
        TestProjection projection = testActor.underlyingActor();

        testActor.tell(call("isSubscribePhase"), super.testActor());
        expectMsg(false);

        EventstoreOrder.SearchRequest request = EventstoreOrder.SearchRequest.newBuilder()
                .setQuery(UUID.randomUUID().toString())
                .setPageNumber(ThreadLocalRandom.current().nextInt(100) + 1)
                .setResultPerPage(ThreadLocalRandom.current().nextInt(100) + 1)
                .build();
        Messages.EventWrapper requestWrapper = ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), request);
        journal.saveEvent(requestWrapper);

        EventstoreOrder.SearchResult result = EventstoreOrder.SearchResult.newBuilder()
                .addResult(UUID.randomUUID().toString())
                .addResult(UUID.randomUUID().toString())
                .addResult(UUID.randomUUID().toString())
                .build();
        Messages.EventWrapper resultWrapper = ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), result);
        journal.saveEvent(resultWrapper);

        new TestInvoker().invoke(() -> {
            assertTrue(projection.receivedSearchRequest(request));
            assertTrue(projection.receivedSearchResult(result));
            assertTrue(projection.hasCorrectCurrentMessage(request, requestWrapper));
            assertTrue(projection.hasCorrectCurrentMessage(result, resultWrapper));
        });
    }



    @Subscriber("no.ks.events.svarut.Order")
    private static class TestProjection extends Projection {

        private List<EventstoreOrder.SearchRequest> handledRequestEvents = new ArrayList<>();
        private List<EventstoreOrder.SearchResult> handledResultEvents = new ArrayList<>();
        private Map<Message, Messages.EventWrapper> eventWrapperMap = new HashMap<>();

        public TestProjection(ActorRef connection) {
            super(connection);
        }

        boolean receivedSearchRequest(EventstoreOrder.SearchRequest expectedEvent) {
            return handledRequestEvents.stream().filter(e ->
                    e.getQuery().equals(expectedEvent.getQuery())
                            && e.getPageNumber() == expectedEvent.getPageNumber()
                            && e.getResultPerPage() == expectedEvent.getResultPerPage()).count()
                    == 1;
        }

        boolean receivedSearchResult(EventstoreOrder.SearchResult expectedEvent) {
            return handledResultEvents.stream().filter(e ->
                    e.getResultList().equals(expectedEvent.getResultList())
                            && e.getNumberOfResults() == expectedEvent.getNumberOfResults()).count()
                    == 1;
        }

        boolean hasCorrectCurrentMessage(Message message, Messages.EventWrapper expectedWrapper) {
            Messages.EventWrapper actualWrapper = eventWrapperMap.get(message);
            return actualWrapper.getAggregateType().equals(expectedWrapper.getAggregateType())
                    && actualWrapper.getAggregateRootId().equals(expectedWrapper.getAggregateRootId())
                    && actualWrapper.getOccurredOn() == expectedWrapper.getOccurredOn()
                    && actualWrapper.getProtoSerializationType().equals(expectedWrapper.getProtoSerializationType())
                    && actualWrapper.getCreatedByUser().equals(expectedWrapper.getCreatedByUser())
                    && actualWrapper.getJournalid() == expectedWrapper.getJournalid();
        }

        @Handler
        public void handleEvent(EventstoreOrder.SearchRequest event) {
            handledRequestEvents.add(event);
            eventWrapperMap.put(event, currentMessage());
        }

        @Handler
        public void handleEvent(EventstoreOrder.SearchResult event) {
            handledResultEvents.add(event);
            eventWrapperMap.put(event, currentMessage());
        }

    }
}
