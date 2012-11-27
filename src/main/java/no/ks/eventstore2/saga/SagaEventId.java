package no.ks.eventstore2.saga;

import no.ks.eventstore2.Event;

public class SagaEventId {
	private final Class<? extends Saga> sagaClass;
	private final Class<? extends Event> eventClass;

	public SagaEventId(Class<? extends Saga> sagaClass, Class<? extends Event> eventClass) {
		this.sagaClass = sagaClass;
		this.eventClass = eventClass;
	}

	public Class<? extends Saga> getSagaClass() {
		return sagaClass;
	}

	public Class<? extends Event> getEventClass() {
		return eventClass;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SagaEventId that = (SagaEventId) o;

		if (eventClass != null ? !eventClass.equals(that.eventClass) : that.eventClass != null) return false;
		if (sagaClass != null ? !sagaClass.equals(that.sagaClass) : that.sagaClass != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = sagaClass != null ? sagaClass.hashCode() : 0;
		result = 31 * result + (eventClass != null ? eventClass.hashCode() : 0);
		return result;
	}
}
