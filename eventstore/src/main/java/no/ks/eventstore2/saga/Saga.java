package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;

public abstract class Saga extends UntypedActor {

	private Logger log = LoggerFactory.getLogger(this.getClass());

	public static final byte STATE_INITIAL = 0;
	public static final byte STATE_FINISHED = Byte.MAX_VALUE;

	protected String id;

	private byte state = STATE_INITIAL;
	protected ActorRef commandDispatcher;
    protected SagaRepository repository;
	private HashMap<Class<? extends Event>, Method> handleEventMap;

	public Saga(String id, ActorRef commandDispatcher, SagaRepository repository) {
		this.id = id;
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
	}

    @Override
    public void preStart() throws Exception {
        super.preStart();
        loadPersistedState();
		init();
    }

	@Override
	public void onReceive(Object o) {
		try{
            if(o instanceof Event){
                Method method = HandlerFinder.findHandlingMethod(handleEventMap, (Event) o);
                method.invoke(this,o);
            }
        } catch(Exception e){
            log.error("Saga threw exception when handling message: ", e);
            throw new RuntimeException("Saga threw exception when handling message: ", e);
        }
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
		try {
			repository.saveState(this.getClass(), id, state);
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
		handleEventMap = new HashMap<Class<? extends Event>, Method>();
		try {
			Class<? extends Saga> projectionClass = this.getClass();
			ListensTo annotation = projectionClass.getAnnotation(ListensTo.class);
			if (annotation != null) {
				EventIdBind[] handledEventClasses = annotation.value();
				for (EventIdBind eventIdBind : handledEventClasses) {
					Method handleEventMethod = null;
					handleEventMethod = projectionClass.getMethod("handleEvent", eventIdBind.eventClass());
					handleEventMap.put(eventIdBind.eventClass(), handleEventMethod);
				}


			}

            handleEventMap.putAll(HandlerFinder.getEventHandlers(this.getClass()));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
