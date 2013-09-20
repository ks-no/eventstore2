package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.eventstore2.command.CommandHandler;
import no.ks.eventstore2.command.HandlesCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@HandlesCommand(ParseForm.class)
public class FormParser extends CommandHandler {

    private static Logger log = LoggerFactory.getLogger(FormParser.class);
    public FormParser(ActorRef eventStore) {
        super(eventStore);
        log.debug("FormParser created");
    }

    public void handleCommand(ParseForm command){
        log.debug("got command " + command);
        eventStore.tell(new FormParsed(command.getFormId()), self());
    }
}
