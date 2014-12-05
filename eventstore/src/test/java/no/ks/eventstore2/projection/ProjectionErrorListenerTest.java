package no.ks.eventstore2.projection;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.nb.mksystem.MKSystemTestKit;import no.nb.mksystem.common.utils.IdUtil;
import no.nb.mksystem.config.ProjectionError;
import no.nb.mksystem.config.ProjectionErrorListener;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProjectionErrorListenerTest extends MKSystemTestKit{


    private TestActorRef<ProjectionErrorListener> projectionErrorListener;

    @Before
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
