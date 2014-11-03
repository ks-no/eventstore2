package no.ks.eventstore2.projection;

import akka.actor.ActorRef;

import java.io.Serializable;

public class ProjectionFailedError implements Serializable {
    private ActorRef projection;
    private Throwable reason;
    private Object message;

    public ProjectionFailedError(ActorRef projection, Throwable reason, Object message) {
        this.projection = projection;
        this.message = message;
        this.reason = reason;
    }

    public ProjectionFailedError(ActorRef projection, Throwable reason) {
        this.projection = projection;
        this.reason = reason;
    }

    public ActorRef getProjection() {
        return projection;
    }

    public void setProjection(ActorRef projection) {
        this.projection = projection;
    }

    public Throwable getReason() {
        return reason;
    }

    public void setReason(Throwable reason) {
        this.reason = reason;
    }

    public Object getMessage() {
        return message;
    }

    public void setMessage(Object message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "ProjectionFailedError{" +
                "projection=" + projection +
                ", reason=" + reason +
                ", message=" + message +
                '}';
    }
}
