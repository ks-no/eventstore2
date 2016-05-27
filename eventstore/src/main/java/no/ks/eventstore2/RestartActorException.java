package no.ks.eventstore2;

public class RestartActorException extends RuntimeException{
    public RestartActorException() {
    }

    public RestartActorException(String message) {
        super(message);
    }

    public RestartActorException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestartActorException(Throwable cause) {
        super(cause);
    }

    public RestartActorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
