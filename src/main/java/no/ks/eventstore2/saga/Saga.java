package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public abstract class Saga extends UntypedActor {


	public static final byte STATE_INITIAL = 0;
	public static final byte STATE_FINISHED = Byte.MAX_VALUE;

	protected String id;

	private byte state = STATE_INITIAL;
	protected ActorRef commandDispatcher;
    protected SagaRepository repository;

    public Saga(String id, ActorRef commandDispatcher, SagaRepository repository) {
		this.id = id;
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
	}

    @Override
    public void preStart() {
        super.preStart();
        loadPersistedState();
    }

    private void loadPersistedState() {
        state = repository.getState(this.getClass(),id);
    }

    public String getId() {
		return id;
	}

	public byte getState() {
		return state;
	}

	protected void transitionState(byte state){
		this.state = state;
        repository.saveState(this.getClass(), id, state);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Saga saga = (Saga) o;

		if (id != null ? !id.equals(saga.id) : saga.id != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return id != null ? id.hashCode() : 0;
	}

	public void forceSetState(byte state) {
		this.state = state;
	}

}
