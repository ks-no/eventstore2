package no.ks.eventstore2.saga;

public class State {
    private final Class<? extends Saga> clazz;
    private final String id;
    private final byte state;

    public State(Class<? extends Saga> clazz, String id, byte state) {

        this.clazz = clazz;
        this.id = id;
        this.state = state;
    }

    public Class<? extends Saga> getClazz() {
        return clazz;
    }

    public String getId() {
        return id;
    }

    public byte getState() {
        return state;
    }
}
