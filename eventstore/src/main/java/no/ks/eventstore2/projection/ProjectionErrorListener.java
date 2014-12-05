package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.ask.Asker;

import java.util.ArrayList;
import java.util.List;

public class ProjectionErrorListener extends UntypedActor {

    private static final int ERRORS_SIZE = 100;
    private List<ProjectionError> errors = new ArrayList<>();

    public ProjectionErrorListener() {
    }

    @Override
    public void onReceive(Object o) {
        if (o instanceof ProjectionFailedError) {
            if (errors.size() < ERRORS_SIZE) {
                errors.add(new ProjectionError((ProjectionFailedError) o));
            }
        } else if (o instanceof Call) {
            Call call = (Call) o;
            if("getErrors".equals(call.getMethodName())) {
                sender().tell(errors, self());
            }
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
