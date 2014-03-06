package no.ks.eventstore2;

import akka.actor.DeadLetter;
import akka.actor.UntypedActor;
import no.ks.eventstore2.response.Success;
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
        if(o instanceof DeadLetter && ((DeadLetter) o).message() instanceof Success) return;
        log.warn("Dead letter: {}", o);
    }


}
