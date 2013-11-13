package no.ks.eventstore2.saga;

public class InvalidSagaConfigurationException extends RuntimeException{
    public InvalidSagaConfigurationException(String message) {
        super(message);
    }

    public InvalidSagaConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
