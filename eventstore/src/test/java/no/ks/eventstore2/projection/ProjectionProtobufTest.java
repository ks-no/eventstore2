package no.ks.eventstore2.projection;

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

/**
 * Created by roopan on 07.09.16.
 */
public class ProjectionProtobufTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public ProjectionProtobufTest() {
        super(_system);
    }

    @Test
    public void testProjectionManagerCreatesProjections() throws Exception {

        List<Props> factories = new ArrayList<>();
        factories.add(Props.create(FormStatuses.class, super.testActor()));

        final TestActorRef<ProjectionProtobufManager> ref = TestActorRef.create(_system, ProjectionProtobufManager.mkProps(super.testActor(), factories), "projectionProtobufManager");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FormStatuses.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertNotNull(projectionRef);
    }

}
