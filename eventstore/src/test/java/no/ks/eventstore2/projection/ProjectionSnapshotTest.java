package no.ks.eventstore2.projection;


import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.google.protobuf.Message;
import com.mongodb.MongoClient;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.testkit.MongoDbEventstore2TestKit;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static no.ks.eventstore2.projection.CallProjection.call;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

class ProjectionSnapshotTest extends MongoDbEventstore2TestKit {

    private static Kryo kryo = new Kryo();


    @Test
    void test_that_a_projection_can_save_and_load_snapshot() {
        Message event1 = saveEvent();
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, eventstoreConnection, mongoClient), UUID.randomUUID().toString());
        testActor.tell(call("assertSearchRequest", event1), super.testActor());
        expectMsg("SEARCH_REQUEST_OK");

        TestProjection testProjection = (TestProjection) testActor.underlyingActor();
        int count = testProjection.handledRequestEvents.size();

        testActor.tell(new TakeSnapshot(), testActor);
        Message event2 = saveEvent();

        TestActorRef<Actor> testActorReader = TestActorRef.create(_system, Props.create(TestProjection.class, eventstoreConnection, mongoClient), UUID.randomUUID().toString());
        testActorReader.tell(call("assertSearchRequest", event2), super.testActor());
        expectMsg("SEARCH_REQUEST_OK");

        TestProjection testProjectionRead = (TestProjection) testActorReader.underlyingActor();
        assertThat(testProjectionRead.snapshotData.size(), is(count));
        assertThat(testProjectionRead.snapshotData, hasItem(equalTo(event1)));
        assertThat(testProjectionRead.snapshotData, not(hasItem(equalTo(event2))));
        assertThat(testProjectionRead.handledRequestEvents.size(), is(1));
        assertThat(testProjectionRead.handledRequestEvents, not(hasItem(equalTo(event1))));
        assertThat(testProjectionRead.handledRequestEvents, hasItem(equalTo(event2)));
    }

    private Message saveEvent() {
        EventstoreOrder.SearchRequest request = EventstoreOrder.SearchRequest.newBuilder()
                .setQuery(UUID.randomUUID().toString())
                .setPageNumber(ThreadLocalRandom.current().nextInt(100) + 1)
                .setResultPerPage(ThreadLocalRandom.current().nextInt(100) + 1)
                .build();
        journal.saveEvent(ProtobufHelper.newEventWrapper("order", UUID.randomUUID().toString(), request));
        return request;
    }


    @Subscriber("Order")
    private static class TestProjection extends MongoDbProjection {

        private List<EventstoreOrder.SearchRequest> handledRequestEvents = new ArrayList<>();
        private List<EventstoreOrder.SearchRequest> snapshotData = new ArrayList<>();

        public TestProjection(ActorRef eventstoreConnection, MongoClient client) throws Exception {
            super(eventstoreConnection, client);
            CollectionSerializer serializer = new CollectionSerializer();
            kryo.register(ArrayList.class, serializer);
            kryo.register(ArrayList.class, 10);
            kryo.register(EventstoreOrder.SearchRequest.class, 20);
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

        @Override
        protected byte[] serializeData() {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Output output = new Output(outputStream);
            kryo.writeClassAndObject(output, handledRequestEvents);
            output.close();
            return outputStream.toByteArray();
        }

        @Override
        protected String getSnapshotDataVersion() {
            return "2";
        }

        @Override
        protected void deSerializeData(byte[] bytes) {
            Input input = new Input(bytes);
            snapshotData = (List<EventstoreOrder.SearchRequest>) kryo.readClassAndObject(input);
        }

        @Handler
        public void handleEvent(EventstoreOrder.SearchRequest event) {
            handledRequestEvents.add(event);
        }

    }
}
