package no.ks.eventstore2.eventstore.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.eventstore2.command.CommandHandler;
import no.ks.eventstore2.command.HandlesCommand;

@HandlesCommand(ParseForm.class)
public class FormParser extends CommandHandler {
    public FormParser(ActorRef eventStore) {
        super(eventStore);
    }

    public void handleCommand(ParseForm command){
        eventStore.tell(new FormParsed(command.getFormId()), self());
    }
}
