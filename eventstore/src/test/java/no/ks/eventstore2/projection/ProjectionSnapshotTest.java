package no.ks.eventstore2.projection;


import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.Subscription;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class ProjectionSnapshotTest extends Eventstore2TestKit {

    private static Kryo kryo = new Kryo();

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("target/snapshot"));
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File("target/snapshot"));
    }

    @Test
    public void test_that_a_projection_can_save_snapshot() throws Exception {
        TestActorRef<Actor> testActor = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(),"target/snapshot"), UUID.randomUUID().toString());
        expectMsgClass(Subscription.class);
        testActor.tell(new TestEvent(), super.testActor());
        testActor.tell(new TakeSnapshot(), super.testActor());

        TestProjection testProjection = (TestProjection) testActor.underlyingActor();

        assertTrue(testProjection.testEventRecieved);
        assertTrue(testProjection.data.size() == 1);

        TestActorRef<Actor> testActorReader = TestActorRef.create(_system, Props.create(TestProjection.class, super.testActor(),"target/snapshot"), UUID.randomUUID().toString());
        expectMsg(new Subscription("TestAggregate","000000001"));

        TestProjection testProjectionRead = (TestProjection) testActorReader.underlyingActor();
        assertTrue(testProjectionRead.data.size() == 1);
    }



    @Subscriber("TestAggregate")
    private static class TestProjection extends LevelDbProjection {

        public boolean testEventRecieved = false;
        private Map<String, Event> data = new HashMap<String, Event>();

        public TestProjection(ActorRef eventStore,String leveldbDirectory) {
            super(eventStore,leveldbDirectory);
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
            return "1";
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
        TestEvent() {
            setAggregateId("TestAggregate");
            setJournalid("000000001");
        }

        @Override
        public String getLogMessage() {
            return null;
        }
    }
}
