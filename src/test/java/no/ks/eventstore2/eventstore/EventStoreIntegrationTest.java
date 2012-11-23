package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class EventStoreIntegrationTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        system.shutdown();
    }

    @Test
    public void testPublishSubscribe() {
        new JavaTestKit(system) {{
            final Props eventStoreProps = new Props(EventStore.class);
            final ActorRef eventStoreRef = system.actorOf(eventStoreProps, "eventStore");

            final Props projectionAProps = new Props(ProjectionA.class);
            final ActorRef projectionARef = system.actorOf(projectionAProps, "projection_A");

            final Props projectionBProps = new Props(ProjectionB.class);
            final ActorRef projectionBRef = system.actorOf(projectionBProps, "projection_B");

            eventStoreRef.tell(new EventA1(), getRef());
            eventStoreRef.tell(new EventA1(), getRef());
            eventStoreRef.tell(new EventA2(), getRef());
            eventStoreRef.tell(new EventA2(), getRef());


            new Within(duration("3 seconds")) {
                protected void run() {
                    final Future<Object> futureEventsA = ask(projectionARef, "getProjectedEvents", 3000);
                    final Future<Object> futureEventsB = ask(projectionBRef, "getProjectedEvents", 3000);

                    new AwaitCond() {
                        protected boolean cond() {
                            return futureEventsA.isCompleted() && futureEventsB.isCompleted();
                        }
                    };

                    assertTrue(futureEventsA.isCompleted() && futureEventsB.isCompleted());

                    try {
                        assertEquals(2, ((ImmutableSet<Event>) Await.result(futureEventsA, Duration.Zero())).size());
                        assertEquals(4, ((ImmutableSet<Event>) Await.result(futureEventsB, Duration.Zero())).size());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            };
        }};
    }
}

