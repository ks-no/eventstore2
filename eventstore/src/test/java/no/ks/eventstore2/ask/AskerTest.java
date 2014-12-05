package no.ks.eventstore2.ask;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.testapplication.TestEvent;
import no.ks.eventstore2.testapplication.TestProjection;
import no.ks.eventstore2.testkit.EventStoreTestKit;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;

public class AskerTest extends EventStoreTestKit {

    private TestActorRef<TestProjection> projection;

    @Before
    public void before() {
        projection = createProjectionRef(Props.create(TestProjection.class, testActor()));
    }

    @Test
    public void testThatAskProjectionWorks() throws Exception {
        List<TestEvent> testEvents = Asker.askProjection(projection, "getEvents", "arg1", "arg2").list(TestEvent.class);
        assertNotNull(testEvents);
    }

    @Test(expected = RuntimeException.class)
    public void testThatAskProjectionWithNullValuesForParamsThrowsRuntimeException() throws Exception {
        Asker.askProjection(projection, "getEvents", "arg1", null).list(TestEvent.class);
    }

    @Test(expected = RuntimeException.class)
    public void testThatAskProjectionWithNullForProjectionThrowRuntimeException() throws Exception {
        Asker.askProjection(null, "getEvents", "arg1", "arg2").list(TestEvent.class);
    }

    @Test(expected = RuntimeException.class)
    public void testThatAskProjectionWithNullForMethodThrowRuntimeException() throws Exception {
        Asker.askProjection(projection, null, "arg1", "arg2").list(TestEvent.class);
    }
}
