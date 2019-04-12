package no.ks.eventstore2.saga;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class Saga extends AbstractActor {

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
	public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
		try {
			super.aroundReceive(receive, msg);
		} catch (Exception e) {
			log.error("Saga threw exception when handling message: ", e);
			throw new RuntimeException("Saga threw exception when handling message: ", e);
		}
	}

	@Override
	public Receive createReceive() {
		return createReceiveBuilder().build();
	}

	protected ReceiveBuilder createReceiveBuilder() {
		return receiveBuilder()
				.match(Messages.EventWrapper.class, this::handleEventWrapper);
	}

	private void handleEventWrapper(Messages.EventWrapper event) throws InvocationTargetException, IllegalAccessException {
		log.trace("Received EventWrapper: {}", event);
		currentEventWrapper = event;

		final Message message = ProtobufHelper.unPackAny(event.getProtoSerializationType(), event.getEvent());
		Method method = HandlerFinder.findHandlingMethod(handleEventMap, message);
		method.invoke(this, message);
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
