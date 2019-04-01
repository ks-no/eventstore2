package no.ks.eventstore2.projection;

public class ProjectionFailedException extends RuntimeException {

    private ProjectionFailedError error;

    ProjectionFailedException(ProjectionFailedError error, Throwable cause) {
        super(cause);
        this.error = error;
    }

    public ProjectionFailedError getError() {
        return error;
    }
}
