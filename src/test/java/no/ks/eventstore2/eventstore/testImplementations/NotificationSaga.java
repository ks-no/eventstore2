package no.ks.eventstore2.eventstore.testImplementations;

import akka.actor.ActorRef;
import no.ks.eventstore2.saga.EventIdBind;
import no.ks.eventstore2.saga.ListensTo;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaRepository;

@ListensTo(value = {
		@EventIdBind(eventClass = LetterReceived.class, idProperty = "letterId"),
		@EventIdBind(eventClass = NotificationSendt.class, idProperty = "letterId")
}, aggregates = "notification")
public class NotificationSaga extends Saga {

	public NotificationSaga(String id, ActorRef commandDispatcher, SagaRepository repository) {
		super(id, commandDispatcher, repository);
	}

	public void handleEvent(LetterReceived event) {
		if (getState() == STATE_INITIAL) {
			transitionState((byte) 2);
			commandDispatcher.tell("Send notification command", self());
		}
	}

	public void handleEvent(NotificationSendt event) {
		if (getState() == 2) {
			transitionState((byte) 3);
			commandDispatcher.tell("Update logs", self());
		}

	}

	@Override
	public void onReceive(Object o) throws Exception {
		if ("BOOM!".equals(o)) {
			throw new RuntimeException("BOOM!");
		} else
			super.onReceive(o);
	}
}
