package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static no.ks.eventstore2.projection.CallProjection.call;

public class EventstoreProjectionSubscriptionTest extends EventstoreEventstore2TestKit {

    @Test
    public void testProjectionCanReceiveEventsAndAnswerCall() {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, this.eventstoreConnection), UUID.randomUUID().toString());

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

        testActor.tell(call("assertSearchRequest", request), super.testActor());
        expectMsg("SEARCH_REQUEST_OK");
        testActor.tell(call("assertSearchResult", result), super.testActor());
        expectMsg("SEARCH_RESULT_OK");
        testActor.tell(call("assertCurrentMessage", request, requestWrapper), super.testActor());
        expectMsg("CURRENT_MESSAGE_OK");
        testActor.tell(call("assertCurrentMessage", result, resultWrapper), super.testActor());
        expectMsg("CURRENT_MESSAGE_OK");
    }



    @Subscriber("Order")
    private static class TestProjection extends Projection {

        private List<EventstoreOrder.SearchRequest> handledRequestEvents = new ArrayList<>();
        private List<EventstoreOrder.SearchResult> handledResultEvents = new ArrayList<>();
        private Map<Message, Messages.EventWrapper> eventWrapperMap = new HashMap<>();

        public TestProjection(ActorRef connection) {
            super(connection);
        }

        public String assertSearchRequest(EventstoreOrder.SearchRequest expectedEvent) {
            long matches = handledRequestEvents.stream().filter(e ->
                    e.getQuery().equals(expectedEvent.getQuery())
                            && e.getPageNumber() == expectedEvent.getPageNumber()
                            && e.getResultPerPage() == expectedEvent.getResultPerPage()
            ).count();

            if (matches == 1) {
                return "SEARCH_REQUEST_OK";
            }
            return "SEARCH_REQUEST_FAILURE";
        }

        public String assertSearchResult(EventstoreOrder.SearchResult expectedEvent) {
            long matches = handledResultEvents.stream().filter(e ->
                    e.getResultList().equals(expectedEvent.getResultList())
                            && e.getNumberOfResults() == expectedEvent.getNumberOfResults()
            ).count();

            if (matches == 1) {
                return "SEARCH_RESULT_OK";
            }
            return "SEARCH_RESULT_FAILURE";
        }

        public String assertCurrentMessage(Message message, Messages.EventWrapper expectedWrapper) {
            Messages.EventWrapper actualWrapper = eventWrapperMap.get(message);
            if (actualWrapper.getAggregateType().equals(expectedWrapper.getAggregateType())
                    && actualWrapper.getAggregateRootId().equals(expectedWrapper.getAggregateRootId())
                    && actualWrapper.getOccurredOn() == expectedWrapper.getOccurredOn()
                    && actualWrapper.getProtoSerializationType().equals(expectedWrapper.getProtoSerializationType())
                    && actualWrapper.getCreatedByUser().equals(expectedWrapper.getCreatedByUser())
                    && actualWrapper.getJournalid() == expectedWrapper.getJournalid()) {
                return "CURRENT_MESSAGE_OK";
            }
            return "CURRENT_MESSAGE_FAILURE";
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
