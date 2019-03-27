package no.ks.eventstore2.projection;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.testkit.EventStoreTestKit;
import no.ks.eventstore2.util.IdUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ProjectionErrorListenerTest extends EventStoreTestKit {


    private TestActorRef<ProjectionErrorListener> projectionErrorListener;

    @BeforeEach
    public void before() {
        projectionErrorListener = TestActorRef.create(actorSystem, Props.create(ProjectionErrorListener.class), IdUtil.createUUID());
    }

    @Test
    public void testAskErrorsReturnsEmptyList() {
        List<ProjectionError> projectionErrors = ProjectionErrorListener.askErrors(projectionErrorListener);
        assertNotNull(projectionErrors);
        assertEquals(0, projectionErrors.size());
    }
}
