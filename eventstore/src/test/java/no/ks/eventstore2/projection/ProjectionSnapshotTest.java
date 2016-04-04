package no.ks.eventstore2.projection;


import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.mongodb.MongoClient;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.Subscription;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProjectionSnapshotTest extends MongoDbEventstore2TestKit {

    private static Kryo kryo = new Kryo();


    @Test
    public void test_that_a_projection_can_save_and_load_snapshot() throws Exception {

        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(), mongoClient), UUID.randomUUID().toString());
        expectMsgClass(Subscription.class);
        testActor.tell(new TestEvent(), super.testActor());
        testActor.tell(new TakeSnapshot(), super.testActor());

        TestProjection testProjection = (TestProjection) testActor.underlyingActor();

        assertTrue(testProjection.testEventRecieved);
        assertTrue(testProjection.data.size() == 1);

        TestActorRef<Actor> testActorReader = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(), mongoClient), UUID.randomUUID().toString());
        expectMsg(new Subscription("TestAggregate","000000001"));

        TestProjection testProjectionRead = (TestProjection) testActorReader.underlyingActor();
        assertTrue(testProjectionRead.data.size() == 1);

        Event event = testProjection.data.get("1");
        assertEquals("TestAggregate", event.getAggregateType());
        assertEquals("000000001", event.getJournalid());
    }



    @Subscriber("TestAggregate")
    private static class TestProjection extends MongoDbProjection {

        public boolean testEventRecieved = false;
        private Map<String, Event> data = new HashMap<String, Event>();

        public TestProjection(ActorRef eventStore, MongoClient client) throws Exception {
            super(eventStore, client);
            MapSerializer serializer = new MapSerializer();
            kryo.register(HashMap.class, serializer);
            kryo.register(HashMap.class, 10);
            kryo.register(TestEvent.class, 20);
        }

        @Override
        protected byte[] serializeData() {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Output output = new Output(outputStream);
            kryo.writeClassAndObject(output, data);
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
            data = (Map<String, Event>) kryo.readClassAndObject(input);
        }

        @Handler
        public void handleEvent(TestEvent event){
            testEventRecieved = true;
            data.put("1",event);
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
