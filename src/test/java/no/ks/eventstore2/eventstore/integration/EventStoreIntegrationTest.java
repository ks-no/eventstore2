package no.ks.eventstore2.eventstore.integration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.google.common.collect.ImmutableSet;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.EmbeddedDatabaseTest;
import no.ks.eventstore2.eventstore.EventStoreFactory;
import no.ks.eventstore2.eventstore.testImplementations.EventA1;
import no.ks.eventstore2.eventstore.testImplementations.EventA2;
import no.ks.eventstore2.eventstore.testImplementations.ProjectionA;
import no.ks.eventstore2.eventstore.testImplementations.ProjectionB;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class EventStoreIntegrationTest extends EmbeddedDatabaseTest {

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
    public void testPublishSubscribe() throws Exception {
        new JavaTestKit(system) {{
            EventStoreFactory eventStoreFactory = new EventStoreFactory();
            eventStoreFactory.setDs(db);

            final Props eventStoreProps = new Props(eventStoreFactory);
            final ActorRef eventStoreRef = system.actorOf(eventStoreProps, "eventStore");
            System.out.println("event store created");

            final Props projectionAProps = new Props(ProjectionA.class);
            final ActorRef projectionARef = system.actorOf(projectionAProps, "projection_A");
            System.out.println("projection a created");

            final Props projectionBProps = new Props(ProjectionB.class);
            final ActorRef projectionBRef = system.actorOf(projectionBProps, "projection_B");
            System.out.println("projection b created");


            new Within(duration("3 seconds")) {
                protected void run() {
                    eventStoreRef.tell(new EventA1(), getRef());
                    eventStoreRef.tell(new EventA1(), getRef());
                    eventStoreRef.tell(new EventA2(), getRef());
                    eventStoreRef.tell(new EventA2(), getRef());
                    final Future<Object> future = ask(eventStoreRef,  "ping", 3000);
                    new AwaitCond() {
                        protected boolean cond() {
                            return future.isCompleted();
                        }
                    };
                };
            };


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

