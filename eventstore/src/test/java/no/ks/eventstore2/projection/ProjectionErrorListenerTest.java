package no.ks.eventstore2.projection;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import no.ks.eventstore2.util.IdUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProjectionErrorListenerTest extends EventstoreEventstore2TestKit {


    private TestActorRef<ProjectionErrorListener> projectionErrorListener;

    @BeforeEach
    void before() {
        projectionErrorListener = TestActorRef.create(_system, Props.create(ProjectionErrorListener.class), IdUtil.createUUID());
    }

    @Test
    void testAskErrorsReturnsEmptyList() {
        List<ProjectionError> projectionErrors = ProjectionErrorListener.askErrors(projectionErrorListener);
        assertNotNull(projectionErrors);
        assertEquals(0, projectionErrors.size());
    }
}
