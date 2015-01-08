package no.ks.eventstore2.saga;

public class State {
    private final String sagaStateId;
    private final String id;
    private final byte state;

    public State(String clazz, String id, byte state) {

        this.sagaStateId = clazz;
        this.id = id;
        this.state = state;
    }

    public String getSagaStateId() {
        return sagaStateId;
    }

    public String getId() {
        return id;
    }

    public byte getState() {
        return state;
    }
}
