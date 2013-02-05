package no.ks.eventstore2.projection;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;

import static akka.pattern.Patterns.ask;
import static junit.framework.Assert.assertNotNull;
import static no.ks.eventstore2.projection.CallProjection.call;


public class ProjectionManagerTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public ProjectionManagerTest() {
        super(_system);
    }

    @Test
    public void testProjectionManagerCreatesProjections() throws Exception {

        final ActorRef eventstore = super.testActor();

        ProjectionFactory projectionFactory = new ProjectionFactory(eventstore) {
            @Override
            public Class<? extends Projection> getProjectionClass() {
                return FormStatuses.class;
            }

            public Actor create() throws Exception {
                return new FormStatuses(eventstore);
            }
        };

        List<ProjectionFactory> factories = new ArrayList<ProjectionFactory>();
        factories.add(projectionFactory);

        final TestActorRef<ProjectionManager> ref = TestActorRef.create(_system, new Props(new ProjectionManagerFactory(factories,super.testActor())), "projectionManager");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FormStatuses.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertNotNull(projectionRef);
    }
}
