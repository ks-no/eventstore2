package no.ks.eventstore2.projection;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import no.ks.eventstore2.ask.Asker;

import java.util.ArrayList;
import java.util.List;

public class ProjectionErrorListener extends AbstractActor {

    private static final int ERRORS_SIZE = 100;
    private List<ProjectionError> errors = new ArrayList<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ProjectionFailedError.class, this::handleProjectionFailedError)
                .match(Call.class, this::handleCall)
                .build();
    }

    private void handleProjectionFailedError(ProjectionFailedError error) {
        if (errors.size() < ERRORS_SIZE) {
            errors.add(new ProjectionError(error));
        }
    }

    private void handleCall(Call call) {
        if("getErrors".equals(call.getMethodName())) {
            sender().tell(errors, self());
        }
    }

    public static List<ProjectionError> askErrors(ActorRef projectionErrorListener) {
        try {
            return Asker.askProjection(projectionErrorListener, "getErrors").list(ProjectionError.class);
        } catch (Exception e) {
            throw new RuntimeException("Kunne ikke hente feil fra projectionErrorListener", e);
        }
    }
}
