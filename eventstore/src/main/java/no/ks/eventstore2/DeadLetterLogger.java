package no.ks.eventstore2;

import akka.actor.DeadLetter;
import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterLogger extends UntypedActor {

    static final Logger log = LoggerFactory.getLogger(DeadLetterLogger.class);

	@Override
	public void preStart() {
		getContext().system().eventStream().subscribe(self(), DeadLetter.class);
	}

    @Override
    public void onReceive(Object o) throws Exception {
        log.warn("Dead letter: {}", o);
    }


}
