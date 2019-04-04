package no.ks.eventstore2.eventstore;

import com.google.protobuf.InvalidProtocolBufferException;
import events.Aggevents.Agg;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TestInvoker;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private EventstoreJournalStorage journal;

    private static final String ORDER_CATEGORY = "events.test.Order";

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        ProtobufHelper.registerDeserializeMethod(Agg.Aggevent.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(Order.SearchRequest.getDefaultInstance());
        journal = new EventstoreJournalStorage(eventstoreConnection);
    }

    @Test
    void testSaveAndRetrieve() throws InvalidProtocolBufferException {
        Order.SearchRequest searchRequest = buildSearchRequest();
        String aggregateRootId = UUID.randomUUID().toString();
        String aggregateType = "order";
        Messages.EventWrapper eventWrapper = ProtobufHelper.newEventWrapper(aggregateType, aggregateRootId, searchRequest);

        Messages.EventWrapper savedEvent = journal.saveEvent(eventWrapper);

        assertThat(savedEvent.getAggregateType(), is(aggregateType));
        assertThat(savedEvent.getAggregateRootId(), is(aggregateRootId));
        assertThat(savedEvent.getEvent().unpack(Order.SearchRequest.class), is(searchRequest));

        new TestInvoker().invoke(() -> {
            Messages.EventWrapper lastEvent = getLastEvent(ORDER_CATEGORY);
            assertEventEquals(lastEvent, savedEvent);
            assertThat(lastEvent.getVersion(), is(savedEvent.getVersion()));
        });
    }

    @Test
    void testSaveAndRetrieveMultipleEvents() {
        String aggregateId = UUID.randomUUID().toString();
        Messages.EventWrapper event1 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
        Messages.EventWrapper event2 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchRequest());
        Messages.EventWrapper event3 = ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchResult());
        List<Messages.EventWrapper> savedEvents = journal.saveEventsBatch(Arrays.asList(event1, event2, event3));

        assertThat(savedEvents.size(), is(3));
        assertEventEquals(savedEvents.get(0), event1);
        assertThat(savedEvents.get(0).getVersion(), is(0L));
        assertEventEquals(savedEvents.get(1), event2);
        assertThat(savedEvents.get(1).getVersion(), is(1L));
        assertEventEquals(savedEvents.get(2), event3);
        assertThat(savedEvents.get(2).getVersion(), is(2L));
    }

    @Test
    void testSaveAndRetrieveMultipleBatches() {
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

        final Order.SearchRequest searchRequest1 = buildSearchRequest();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), searchRequest1));
        new TestInvoker().invoke(() -> assertThat(getLatestJournalId(ORDER_CATEGORY), is(latestJournalId + 1)));

        final Order.SearchRequest searchRequest2 = buildSearchRequest();
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

    @Test
    void testCorrelationId() {
        String correlationId = UUID.randomUUID().toString();

        final Order.SearchRequest searchRequest = buildSearchRequest();
        Messages.EventWrapper searchRequestSaved = journal.saveEvent(ProtobufHelper.newEventWrapper(
                "order", UUID.randomUUID().toString(), searchRequest, correlationId, ""));
        Messages.EventWrapper searchRequestRead = getLastEvent(ORDER_CATEGORY);

        assertThat(searchRequestSaved.getCorrelationId(), is(correlationId));
        assertThat(searchRequestRead.getCorrelationId(), is(correlationId));
    }

    @Test
    void testTwoEventsWithoutVersion() {
        String aggregateId = UUID.randomUUID().toString();
        List<Messages.EventWrapper> savedEvents1 = journal.saveEventsBatch(Arrays.asList(
                ProtobufHelper.newEventWrapper("order", aggregateId, -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
                ProtobufHelper.newEventWrapper("order", aggregateId, -1, Order.SearchResult.newBuilder().addResult("res1").addResult("res2").build())
        ));
        List<Messages.EventWrapper> savedEvents2 = journal.saveEventsBatch(Arrays.asList(
                ProtobufHelper.newEventWrapper("order", aggregateId, -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
                ProtobufHelper.newEventWrapper("order", aggregateId, -1, Order.SearchResult.newBuilder().addResult("res1").addResult("res2").build())
        ));
        assertThat(savedEvents1.size(), is(2));
        assertThat(savedEvents1.get(0).getVersion(), is(0L));
        assertThat(savedEvents1.get(1).getVersion(), is(1L));
        assertThat(savedEvents2.size(), is(2));
        assertThat(savedEvents2.get(0).getVersion(), is(2L));
        assertThat(savedEvents2.get(1).getVersion(), is(3L));
    }

    @Test
    void testSaveAndReceiveEventsFromKey() {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);
        String aggregateId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            batch.add(ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchResult()));
        }
        journal.saveEventsBatch(batch);

        final List<Messages.EventWrapper> events = loadEventsExpectSize(previousJournalId + 1, 100);

        assertEventEquals(events.get(0), batch.get(0));
        assertEventEquals(events.get(batch.size() - 1), batch.get(batch.size() - 1));
    }

    @Test
    void testEventReadLimit() {
        long previousJournalId = getLatestJournalId(ORDER_CATEGORY);
        String aggregateId = UUID.randomUUID().toString();

        List<Messages.EventWrapper> batch = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            batch.add(ProtobufHelper.newEventWrapper("order", aggregateId, buildSearchResult()));
        }
        journal.saveEventsBatch(batch);
        journal.saveEventsBatch(batch);

        final List<Messages.EventWrapper> events = loadEventsExpectSize(previousJournalId + 1, 500);

        assertEventEquals(events.get(0), batch.get(0));
        assertEventEquals(events.get(events.size() - 1), batch.get(events.size() - 1));
    }

    @Test
    void testWritingExistingVersionShouldFail() {
        String aggregateId = UUID.randomUUID().toString();
        Order.SearchRequest searchRequest = buildSearchRequest();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateId, -1, searchRequest));

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateId, 1, searchRequest)));

        assertThat(exception.getMessage(), containsString("Write failed due to WrongExpectedVersion"));
    }

    @Test
    void testStresstest() throws InterruptedException, ExecutionException, TimeoutException {
        long nextJournalId = getLatestJournalId(ORDER_CATEGORY) + 1;

        final int numberOfVersions = 50;
        final int numberOfAggregates = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final ArrayList<Future<String>> futures = new ArrayList<>();
        for (int p = 0; p < numberOfAggregates; p++) {
            futures.add(executorService.submit(() -> {
                String aggregateRootId = UUID.randomUUID().toString();
                for (int i = 0; i < numberOfVersions; i++) {
                    journal.saveEvent(ProtobufHelper.newEventWrapper("order", aggregateRootId, -1, Order.SearchRequest.newBuilder().build()));
                }
                return aggregateRootId;
            }));
        }
        final ArrayList<String> aggregateIds = new ArrayList<>();
        for (Future<String> future : futures) {
            aggregateIds.add(future.get(60, TimeUnit.SECONDS));
        }
        final ArrayList<Messages.EventWrapper> events = new ArrayList<>();

        boolean finished = journal.loadEventsAndHandle("events.test.Order", events::add, nextJournalId);
        while (!finished) {
            nextJournalId += 500;
            finished = journal.loadEventsAndHandle("events.test.Order", events::add, nextJournalId);
        }
        assertThat(events.size(), is(numberOfAggregates * numberOfVersions));
    }

    @Test
    void testTrySaveMultipleAggregateIdsInSingleBatch() {
        final List<Messages.EventWrapper> events = Arrays.asList(
                ProtobufHelper.newEventWrapper("agg1", "1", -1,
                        Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()),
                ProtobufHelper.newEventWrapper("agg1", "2", -1,
                        Order.SearchResult.newBuilder().addResult("res1").addResult("res2").build()));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> journal.saveEventsBatch(events));
        assertThat(exception.getMessage(), containsString("Can't save batch with multiple aggregate root ids"));
    }

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
