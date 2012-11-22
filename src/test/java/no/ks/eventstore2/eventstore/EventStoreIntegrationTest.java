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
    public void testPublishEvents() {
        new JavaTestKit(system) {{
            final Props eventStoreProps = new Props(EventStore.class);
            final ActorRef eventStoreRef = system.actorOf(eventStoreProps);

            final Props eventProjectionProps = new Props(EventProjection.class);
            final ActorRef eventProjectionRef = system.actorOf(eventProjectionProps);

            eventStoreRef.tell(new Event(), getRef());
            eventStoreRef.tell(new Event(), getRef());
            eventStoreRef.tell(new Event(), getRef());
            eventStoreRef.tell(new Event(), getRef());
            eventStoreRef.tell(new Event(), getRef());
            eventStoreRef.tell("publishEvents", getRef());

            new Within(duration("3 seconds")) {
                protected void run() {
                    final Future<Object> future = ask(eventProjectionRef, "getProjectedEvents", 3000);

                    new AwaitCond() {
                        protected boolean cond() {
                            return future.isCompleted();
                        }
                    };

                    assertTrue(future.isCompleted());

                    try {
                        assertEquals(5, ((ImmutableSet<Event>) Await.result(future, Duration.Zero())).size());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            };
        }};
    }
}

