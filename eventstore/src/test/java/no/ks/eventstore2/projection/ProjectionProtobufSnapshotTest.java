package no.ks.eventstore2.projection;


import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.MongoClient;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.Subscription;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProjectionProtobufSnapshotTest extends MongoDbEventstore2TestKit {

    private static Kryo kryo = new Kryo();


    @Test
    public void test_that_a_projection_can_save_and_load_snapshot() throws Exception {

        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(), mongoClient), UUID.randomUUID().toString());
        expectMsgClass(Messages.AsyncSubscription.class);
        testActor.tell(Messages.CompleteSubscriptionRegistered.newBuilder().setAggregateType("TestAggregate").build(),super.testActor());
        testActor.tell(ProtobufHelper.newEventWrapper("TestAggregate","id", 1, Order.SearchResult.newBuilder().getDefaultInstanceForType()).toBuilder().setJournalid(1).build(), super.testActor());
        testActor.tell(new TakeSnapshot(), super.testActor());

        TestProjection testProjection = (TestProjection) testActor.underlyingActor();

        assertTrue(testProjection.testEventRecieved);
        assertTrue(testProjection.data != null);

        TestActorRef<Actor> testActorReader = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(), mongoClient), UUID.randomUUID().toString());
        final Messages.AsyncSubscription subscriptionMessage = Messages.AsyncSubscription.newBuilder().setFromJournalId(1).setAggregateType("TestAggregate").build();
        expectMsg(subscriptionMessage);

        TestProjection testProjectionRead = (TestProjection) testActorReader.underlyingActor();
        assertTrue(testProjectionRead.data != null);


        assertEquals("TestAggregate", testProjectionRead.data.getAggregateType());
        assertEquals(1, testProjectionRead.latestJournalidReceived);
    }

    @Subscriber("TestAggregate")
    private static class TestProjection extends MongoDbProtobufProjection {

        public boolean testEventRecieved = false;
        private Messages.EventWrapper data = null;

        public TestProjection(ActorRef eventStore, MongoClient client) throws Exception {
            super(eventStore, client);
        }

        @Override
        protected byte[] serializeData() {
            if(data == null)
                return null;
            return data.toByteArray();
        }

        @Override
        protected String getSnapshotDataVersion() {
            return "2";
        }

        @Override
        protected void deSerializeData(byte[] bytes) {
            try {
                data = Messages.EventWrapper.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }

        @Handler
        public void handleEvent(Order.SearchResult event){
            testEventRecieved = true;
            data = currentMessage();
        }

    }
    private static class TestEvent extends Event {
		private static final long serialVersionUID = 1L;

		TestEvent() {
            setJournalid("000000001");
        }

        @Override
        public String getLogMessage() {
            return null;
        }

        @Override
        public String getAggregateRootId() {
            return null;
        }
        
        @Override
    	public String getAggregateType() {
    		return "TestAggregate";
    	}
    }
}
