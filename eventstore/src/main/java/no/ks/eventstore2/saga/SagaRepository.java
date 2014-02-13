package no.ks.eventstore2.saga;

public abstract class SagaRepository {

	public abstract void saveState(Class<? extends Saga> clz, String sagaid, byte state);

    public abstract byte getState(Class<? extends Saga> clz, String sagaid);

    public abstract void close();

    public abstract void open();
}
