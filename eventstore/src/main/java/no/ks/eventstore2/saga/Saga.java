package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class Saga extends UntypedActor {

	private Logger log = LoggerFactory.getLogger(this.getClass());

	public static final byte STATE_INITIAL = 0;
	public static final byte STATE_FINISHED = Byte.MAX_VALUE;

	protected String id;

	private byte state = STATE_INITIAL;
	protected ActorRef commandDispatcher;
    protected SagaRepository repository;
	private Map<Class<? extends Message>, Method> handleEventMap = new HashMap<>();

	public Saga(String id, ActorRef commandDispatcher, SagaRepository repository) {
		this.id = id;
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
	}

	/**
	 * Get a unique id identifying this saga
	 * SagaStateId and id identifies state in saga repository.
	 * @return unique id
	 */
	protected abstract String getSagaStateId();

    @Override
    public void preStart() throws Exception {
        super.preStart();
        loadPersistedState();
		init();
    }

    private Messages.EventWrapper currentEventWrapper= null;

    protected Messages.EventWrapper eventWrapper(){
		return currentEventWrapper;
	}

	@Override
	public void onReceive(Object o) {
		try {
			if(o instanceof Messages.EventWrapper) {
				log.debug("Received event {}", o);
				currentEventWrapper = (Messages.EventWrapper) o;

				final Message message = ProtobufHelper.unPackAny(((Messages.EventWrapper) o).getProtoSerializationType(), ((Messages.EventWrapper) o).getEvent());
				Method method = HandlerFinder.findHandlingMethod(handleEventMap, message);
				method.invoke(this, message);
			} else if (o instanceof Message) {
				Method method = HandlerFinder.findHandlingMethod(handleEventMap, (Message) o);
				method.invoke(this, (Message) o);
			}
        } catch(Exception e) {
            log.error("Saga threw exception when handling message: ", e);
            throw new RuntimeException("Saga threw exception when handling message: ", e);
        }
	}

	private void loadPersistedState() {
		state = repository.getState(getSagaStateId(), id);
    }

    public String getId() {
		return id;
	}

	public byte getState() {
		return state;
	}

	protected void transitionState(byte state){
        if (getState() == state) {
        	return;
        }
		this.state = state;
		try {
			repository.saveState(getSagaStateId(), id, state);
		} catch(Exception e){
			log.error("Failed to save state for class " + this.getClass() + " id "  + id + " state " + state);
			throw new RuntimeException(e);
		}
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

	private void init() {
		handleEventMap = new HashMap<>();
		handleEventMap.putAll(HandlerFinder.getEventHandlers(this.getClass()));
	}
}
