package no.ks.eventstore2.eventstore.formProcessor;

import akka.actor.ActorRef;
import no.ks.eventstore2.command.CommandHandler;
import no.ks.eventstore2.command.HandlesCommand;

@HandlesCommand(DeliverForm.class)
public class FormDeliverer extends CommandHandler {
    public FormDeliverer(ActorRef eventStore) {
        super(eventStore);
    }

    public void handleCommand(DeliverForm command){
        eventStore.tell(new FormDelivered(command.getFormId()), self());
    }
}
