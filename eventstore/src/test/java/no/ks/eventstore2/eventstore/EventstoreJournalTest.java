package no.ks.eventstore2.eventstore;

import events.Aggevents.Agg;
import eventstore.Messages;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EventstoreJournalTest extends EventstoreEventstore2TestKit {

    private static final int TEST_BATCH_SIZE_LIMIT = 10;
    private EventStoreJournalStorage journal;

    private static final String ORDER_CATEGORY = "no.ks.events.svarut.Order";

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        ProtobufHelper.registerDeserializeMethod(Agg.Aggevent.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchRequest.getDefaultInstance());
        journal = new EventStoreJournalStorage(eventstoreConnection, _system.dispatcher(), TEST_BATCH_SIZE_LIMIT);
    }

//    @Test
//    void testSaveAndRetrieve() throws InvalidProtocolBufferException {
//        EventstoreOrder.SearchRequest searchRequest = buildSearchRequest();
//        String aggregateRootId = UUID.randomUUID().toString();
//        String aggregateType = "order";
//        Messages.EventWrapper eventWrapper = ProtobufHelper.newEventWrapper(aggregateType, aggregateRootId, searchRequest);
//
//        Messages.EventWrapper savedEvent = journal.saveEvent(eventWrapper);
//
//        assertThat(savedEvent.getAggregateType(), is(aggregateType));
//        assertThat(savedEvent.getAggregateRootId(), is(aggregateRootId));
//        assertThat(savedEvent.getEvent().unpack(EventstoreOrder.SearchRequest.class), is(searchRequest));
//
//        new TestInvoker().invoke(() -> {
//            Messages.EventWrapper lastEvent = getLastEvent(ORDER_CATEGORY);
//            assertEventEquals(lastEvent, savedEvent);
//            assertThat(lastEvent.getVersion(), is(savedEvent.getVersion()));
//        });
//    }
//
//    @Test
//    void testSaveAndRetrieveMultipleEvents() {
//        String aggregateId = UUID.randomUUID().toString();
//        Messages.EventWrapper event1 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
//        Messages.EventWrapper event2 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
//        Messages.EventWrapper event3 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchResult());
//        List<Messages.EventWrapper> savedEvents = journal.saveEventsBatch(Arrays.asList(event1, event2, event3));
//
//        assertThat(savedEvents.size(), is(3));
//        assertEventEquals(savedEvents.get(0), event1);
//        assertThat(savedEvents.get(0).getVersion(), is(0L));
//        assertEventEquals(savedEvents.get(1), event2);
//        assertThat(savedEvents.get(1).getVersion(), is(1L));
//        assertEventEquals(savedEvents.get(2), event3);
//        assertThat(savedEvents.get(2).getVersion(), is(2L));
//    }

    @Test
    void testSaveAndRetrieveMultipleBatches() throws Exception {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);
        String aggregateId1 = UUID.randomUUID().toString();
        String aggregateId2 = UUID.randomUUID().toString();
        Messages.EventWrapper event1 = ProtobufHelper.newEventWrapper("order", aggregateId1, buildSearchRequest());
        Messages.EventWrapper event2 = ProtobufHelper.newEventWrapper("order", aggregateId1, buildSearchResult());
        Messages.EventWrapper event3 = ProtobufHelper.newEventWrapper("order", aggregateId2, buildSearchRequest());
        Messages.EventWrapper event4 = ProtobufHelper.newEventWrapper("order", aggregateId2, buildSearchResult());
        Messages.EventWrapper event5 = ProtobufHelper.newEventWrapper("order", aggregateId1, buildSearchRequest());
        Messages.EventWrapper event6 = ProtobufHelper.newEventWrapper("order", aggregateId1, buildSearchResult());
        journal.saveEventsBatch(Arrays.asList(event1, event2));
        journal.saveEventsBatch(Arrays.asList(event3, event4));
        journal.saveEventsBatch(Arrays.asList(event5, event6));
        List<Messages.EventWrapper> allEvents = getAllEvents(ORDER_CATEGORY);

        assertThat(allEvents.get(allEvents.size() - 6).getVersion(), is(0L));
        assertThat(allEvents.get(allEvents.size() - 6).getJournalid(), is(previousJournalId + 1));
        assertEventEquals(allEvents.get(allEvents.size() - 6), event1);
        assertThat(allEvents.get(allEvents.size() - 5).getVersion(), is(1L));
        assertThat(allEvents.get(allEvents.size() - 5).getJournalid(), is(previousJournalId + 2));
        assertEventEquals(allEvents.get(allEvents.size() - 5), event2);
        assertThat(allEvents.get(allEvents.size() - 4).getVersion(), is(0L));
        assertThat(allEvents.get(allEvents.size() - 4).getJournalid(), is(previousJournalId + 3));
        assertEventEquals(allEvents.get(allEvents.size() - 4), event3);
        assertThat(allEvents.get(allEvents.size() - 3).getVersion(), is(1L));
        assertThat(allEvents.get(allEvents.size() - 3).getJournalid(), is(previousJournalId + 4));
        assertEventEquals(allEvents.get(allEvents.size() - 3), event4);
        assertThat(allEvents.get(allEvents.size() - 2).getVersion(), is(2L));
        assertThat(allEvents.get(allEvents.size() - 2).getJournalid(), is(previousJournalId + 5));
        assertEventEquals(allEvents.get(allEvents.size() - 2), event5);
        assertThat(allEvents.get(allEvents.size() - 1).getVersion(), is(3L));
        assertThat(allEvents.get(allEvents.size() - 1).getJournalid(), is(previousJournalId + 6));
        assertEventEquals(allEvents.get(allEvents.size() - 1), event6);
    }

    @Test
    void testJournalId() {
        long latestJournalId = getLatestJournalId(ORDER_CATEGORY);

        final EventstoreOrder.SearchRequest searchRequest1 = buildSearchRequest();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), searchRequest1));
        new TestInvoker().invoke(() -> assertThat(getLatestJournalId(ORDER_CATEGORY), is(latestJournalId + 1)));

        final EventstoreOrder.SearchRequest searchRequest2 = buildSearchRequest();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), searchRequest2));
        new TestInvoker().invoke(() -> assertThat(getLatestJournalId(ORDER_CATEGORY), is(latestJournalId + 2)));
    }

    @Test
    void testJournalIdMultipleEvents() {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);

        String aggregateId = UUID.randomUUID().toString();
        Messages.EventWrapper event1 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
        Messages.EventWrapper event2 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
        journal.saveEventsBatch(Arrays.asList(event1, event2));
        List<Messages.EventWrapper> allEvents = getAllEvents(ORDER_CATEGORY);

        assertThat(allEvents.get(allEvents.size() - 2).getJournalid(), is(previousJournalId + 1));
        assertThat(allEvents.get(allEvents.size() - 1).getJournalid(), is(previousJournalId + 2));
    }

//    @Test
//    void testCorrelationId() {
//        String correlationId = UUID.randomUUID().toString();
//
//        final EventstoreOrder.SearchRequest searchRequest = buildSearchRequest();
//        Messages.EventWrapper searchRequestSaved = journal.saveEvent(ProtobufHelper.newEventWrapper(
//                "order", UUID.randomUUID().toString(), searchRequest, correlationId, ""));
//        Messages.EventWrapper searchRequestRead = getLastEvent(ORDER_CATEGORY);
//
//        assertThat(searchRequestSaved.getCorrelationId(), is(correlationId));
//        assertThat(searchRequestRead.getCorrelationId(), is(correlationId));
//    }
//
//    @Test
//    void testTwoEventsWithoutVersion() {
//        String aggregateId = UUID.randomUUID().toString();
//        List<Messages.EventWrapper> savedEvents1 = journal.saveEventsBatch(Arrays.asList(
//                ProtobufHelper.newEventWrapper("order", aggregateId, -1, EventstoreOrder.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
//                ProtobufHelper.newEventWrapper("order", aggregateId, -1, EventstoreOrder.SearchResult.newBuilder().addResult("res1").addResult("res2").build())
//        ));
//        List<Messages.EventWrapper> savedEvents2 = journal.saveEventsBatch(Arrays.asList(
//                ProtobufHelper.newEventWrapper("order", aggregateId, -1, EventstoreOrder.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
//                ProtobufHelper.newEventWrapper("order", aggregateId, -1, EventstoreOrder.SearchResult.newBuilder().addResult("res1").addResult("res2").build())
//        ));
//        assertThat(savedEvents1.size(), is(2));
//        assertThat(savedEvents1.get(0).getVersion(), is(0L));
//        assertThat(savedEvents1.get(1).getVersion(), is(1L));
//        assertThat(savedEvents2.size(), is(2));
//        assertThat(savedEvents2.get(0).getVersion(), is(2L));
//        assertThat(savedEvents2.get(1).getVersion(), is(3L));
//    }

    @Test
    void testSaveAndReceiveEventsFromKey() {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);
        String aggregateId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> batch = new ArrayList<>();
        for (int i = 0; i < TEST_BATCH_SIZE_LIMIT; i++) {
            batch.add(ProtobufHelper.newEventWrapper("order", aggregateId, ThreadLocalRandom.current().nextBoolean()
                    ? buildSearchResult()
                    : buildSearchRequest()));
        }
        journal.saveEventsBatch(batch);

        final List<Messages.EventWrapper> events = loadEventsExpectSize(previousJournalId + 1, TEST_BATCH_SIZE_LIMIT);

        assertEventEquals(events.get(0), batch.get(0));
        assertEventEquals(events.get(batch.size() - 1), batch.get(batch.size() - 1));
    }

    @Test
    void testEventReadLimit() {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);
        String aggregateId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> batch = new ArrayList<>();
        for (int i = 0; i < TEST_BATCH_SIZE_LIMIT; i++) {
            batch.add(ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchResult()));
        }
        journal.saveEventsBatch(batch);
        journal.saveEventsBatch(batch);

        final List<Messages.EventWrapper> events = loadEventsExpectSize(previousJournalId + 1, TEST_BATCH_SIZE_LIMIT);

        assertEventEquals(events.get(0), batch.get(0));
        assertEventEquals(events.get(events.size() - 1), batch.get(events.size() - 1));
    }

    @Test
    void testWritingExistingVersionShouldFail() {
        String aggregateId = UUID.randomUUID().toString();
        EventstoreOrder.SearchRequest searchRequest = buildSearchRequest();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateId, -1, searchRequest));

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateId, 1, searchRequest)));

        assertThat(exception.getMessage(), containsString("Write failed due to WrongExpectedVersion"));
    }

    @Test
    void testStresstest() throws Exception {
        long nextJournalId = getLatestJournalId(ORDER_CATEGORY) + 1;

        final int numberOfVersions = 50;
        final int numberOfAggregates = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final ArrayList<Future<String>> futures = new ArrayList<>();
        for (int p = 0; p < numberOfAggregates; p++) {
            futures.add(executorService.submit(() -> {
                String aggregateRootId = UUID.randomUUID().toString();
                for (int i = 0; i < numberOfVersions; i++) {
                    journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateRootId, -1, EventstoreOrder.SearchRequest.newBuilder().build()));
                }
                return aggregateRootId;
            }));
        }
        final ArrayList<String> aggregateIds = new ArrayList<>();
        for (Future<String> future : futures) {
            aggregateIds.add(future.get(60, TimeUnit.SECONDS));
        }
        final ArrayList<Messages.EventWrapper> events = new ArrayList<>();

        boolean finished = journal.loadEventsAndHandle(ORDER_CATEGORY, events::add, nextJournalId);
        while (!finished) {
            nextJournalId += TEST_BATCH_SIZE_LIMIT;
            finished = journal.loadEventsAndHandle(ORDER_CATEGORY, events::add, nextJournalId);
        }
        assertThat(events.size(), is(numberOfAggregates * numberOfVersions));
    }

    @Test
    void testTrySaveMultipleAggregateIdsInSingleBatch() {
        final List<Messages.EventWrapper> events = Arrays.asList(
                ProtobufHelper.newEventWrapper("agg1", "1", -1,
                        EventstoreOrder.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
                ProtobufHelper.newEventWrapper("agg1", "2", -1,
                        EventstoreOrder.SearchResult.newBuilder().addResult("res1").addResult("res2").build()));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> journal.saveEventsBatch(events));
        assertThat(exception.getMessage(), containsString("Can't save batch with multiple aggregate root ids"));
    }

    @Test
    void testLoadEventWrappersForAggregateId() throws Exception {
        String aggregateRootId = UUID.randomUUID().toString();

        EventstoreOrder.SearchRequest request1 = buildSearchRequest();
        Messages.EventWrapper wrapper1 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request1);
        EventstoreOrder.SearchRequest request2 = buildSearchRequest();
        Messages.EventWrapper wrapper2 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request2);
        journal.saveEventsBatch(Arrays.asList(wrapper1, wrapper2));
        EventstoreOrder.SearchRequest request3 = buildSearchRequest();
        Messages.EventWrapper wrapper3 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request3);
        journal.saveEvent(wrapper3);

        Messages.EventWrapperBatch batch = journal.loadEventWrappersForAggregateId("no.ks.events.svarut.Order", aggregateRootId, 0);
        assertThat(batch.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch.getEventsList().size(), is(3));
        assertEventEquals(batch.getEventsList().get(0), wrapper1);
        assertEventEquals(batch.getEventsList().get(1), wrapper2);
        assertEventEquals(batch.getEventsList().get(2), wrapper3);
    }

    @Test
    void testLoadEventWrappersForAggregateIdWhenMoreEventsThanLimit() throws Exception {
        String aggregateRootId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> events1 = new ArrayList<>();
        List<Messages.EventWrapper> events2 = new ArrayList<>();
        for (int i = 0; i < TEST_BATCH_SIZE_LIMIT; i++) {
            events1.add(ProtobufHelper.newEventWrapper("order", aggregateRootId, buildSearchRequest()));
        }
        for (int i = 0; i < 3; i++) {
            events2.add(ProtobufHelper.newEventWrapper("order", aggregateRootId, buildSearchRequest()));
        }
        journal.saveEventsBatch(events1);
        journal.saveEventsBatch(events2);

        Messages.EventWrapperBatch batch1 = journal.loadEventWrappersForAggregateId("no.ks.events.svarut.Order", aggregateRootId, 0);
        Messages.EventWrapperBatch batch2 = journal.loadEventWrappersForAggregateId("no.ks.events.svarut.Order", aggregateRootId, TEST_BATCH_SIZE_LIMIT);
        assertThat(batch1.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch1.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch1.getEventsList().size(), is(10));
        assertEventEquals(events1.get(0), batch1.getEvents(0));
        assertEventEquals(events1.get(TEST_BATCH_SIZE_LIMIT - 1), batch1.getEvents(TEST_BATCH_SIZE_LIMIT - 1));
        assertThat(batch2.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch2.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch2.getEventsList().size(), is(3));
        assertEventEquals(events2.get(0), batch2.getEvents(0));
        assertEventEquals(events2.get(2), batch2.getEvents(2));
    }

    @Test
    void testLoadEventWrappersForAggregateIdAsync() throws Exception {
        String aggregateRootId = UUID.randomUUID().toString();

        EventstoreOrder.SearchRequest request1 = buildSearchRequest();
        Messages.EventWrapper wrapper1 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request1);
        EventstoreOrder.SearchRequest request2 = buildSearchRequest();
        Messages.EventWrapper wrapper2 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request2);
        journal.saveEventsBatch(Arrays.asList(wrapper1, wrapper2));
        EventstoreOrder.SearchRequest request3 = buildSearchRequest();
        Messages.EventWrapper wrapper3 = ProtobufHelper.newEventWrapper("order", aggregateRootId, request3);
        journal.saveEvent(wrapper3);

        Messages.EventWrapperBatch batch = Await.result(
                journal.loadEventWrappersForAggregateIdAsync("no.ks.events.svarut.Order", aggregateRootId, 0),
                Duration.apply(3, TimeUnit.SECONDS));
        assertThat(batch.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch.getEventsList().size(), is(3));
        assertEventEquals(batch.getEventsList().get(0), wrapper1);
        assertEventEquals(batch.getEventsList().get(1), wrapper2);
        assertEventEquals(batch.getEventsList().get(2), wrapper3);
    }

    @Test
    void testLoadEventWrappersForAggregateIdAsyncWhenMoreEventsThanLimit() throws Exception {
        String aggregateRootId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> events1 = new ArrayList<>();
        List<Messages.EventWrapper> events2 = new ArrayList<>();
        for (int i = 0; i < TEST_BATCH_SIZE_LIMIT; i++) {
            events1.add(ProtobufHelper.newEventWrapper("order", aggregateRootId, buildSearchRequest()));
        }
        for (int i = 0; i < 3; i++) {
            events2.add(ProtobufHelper.newEventWrapper("order", aggregateRootId, buildSearchRequest()));
        }
        journal.saveEventsBatch(events1);
        journal.saveEventsBatch(events2);

        Messages.EventWrapperBatch batch1 = Await.result(
                journal.loadEventWrappersForAggregateIdAsync("no.ks.events.svarut.Order", aggregateRootId, 0),
                Duration.apply(3, TimeUnit.SECONDS));
        Messages.EventWrapperBatch batch2 = Await.result(
                journal.loadEventWrappersForAggregateIdAsync("no.ks.events.svarut.Order", aggregateRootId, TEST_BATCH_SIZE_LIMIT),
                Duration.apply(3, TimeUnit.SECONDS));
        assertThat(batch1.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch1.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch1.getEventsList().size(), is(10));
        assertEventEquals(events1.get(0), batch1.getEvents(0));
        assertEventEquals(events1.get(TEST_BATCH_SIZE_LIMIT - 1), batch1.getEvents(TEST_BATCH_SIZE_LIMIT - 1));
        assertThat(batch2.getAggregateType(), is("no.ks.events.svarut.Order"));
        assertThat(batch2.getAggregateRootId(), is(aggregateRootId));
        assertThat(batch2.getEventsList().size(), is(3));
        assertEventEquals(events2.get(0), batch2.getEvents(0));
        assertEventEquals(events2.get(2), batch2.getEvents(2));
    }

//    @Test
//    void testLoadEventWrappersForCorrelationIdAsync() throws Exception {
//        String correlationId = UUID.randomUUID().toString();
//        String aggregateId1 = UUID.randomUUID().toString();
//        EventstoreOrder.SearchRequest request1 = buildSearchRequest();
//        Messages.EventWrapper wrapper1 = ProtobufHelper.newEventWrapper("order", aggregateId1, request1, correlationId, UUID.randomUUID().toString());
//        EventstoreOrder.SearchRequest request2 = buildSearchRequest();
//        Messages.EventWrapper wrapper2 = ProtobufHelper.newEventWrapper("order", aggregateId1, request2, correlationId, UUID.randomUUID().toString());
//        journal.saveEventsBatch(Arrays.asList(wrapper1, wrapper2));
//        EventstoreOrder.SearchRequest request3 = buildSearchRequest();
//        Messages.EventWrapper wrapper3 = ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), request3, correlationId, UUID.randomUUID().toString());
//        journal.saveEvent(wrapper3);
//
//        Messages.EventWrapperBatch batch = Await.result(
//                journal.loadEventWrappersForCorrelationIdAsync("no.ks.events.svarut.Order", correlationId, 0),
//                Duration.apply(3, TimeUnit.SECONDS));
////        assertThat(batch1.getAggregateType(), is("Order"));
////        assertThat(batch1.getAggregateRootId(), is(aggregateRootId));
////        assertThat(batch1.getEventsList().size(), is(10));
////        assertEventEquals(events1.get(0), batch1.getEvents(0));
////        assertEventEquals(events1.get(TEST_BATCH_SIZE_LIMIT - 1), batch1.getEvents(TEST_BATCH_SIZE_LIMIT - 1));
//    }

    private void assertEventEquals(Messages.EventWrapper first, Messages.EventWrapper second) {
        assertThat(first.getCorrelationId(), is(second.getCorrelationId()));
        assertThat(first.getProtoSerializationType(), is(second.getProtoSerializationType()));
        assertThat(first.getAggregateRootId(), is(second.getAggregateRootId()));
        assertThat(first.getAggregateType(), is(second.getAggregateType()));
        assertThat(first.getOccurredOn(), is(second.getOccurredOn()));
        assertThat(first.getCreatedByUser(), is(second.getCreatedByUser()));
        assertThat(first.getEvent(), is(second.getEvent()));
    }

    private List<Messages.EventWrapper> loadEventsExpectSize(long fromKey, int expectedSize) {
        return new TestInvoker().invoke(() -> {
            final List<Messages.EventWrapper> events = new ArrayList<>();
            journal.loadEventsAndHandle(ORDER_CATEGORY, events::add, fromKey);

            assertThat(events.size(), is(expectedSize));

            return events;
        });
    }
}
