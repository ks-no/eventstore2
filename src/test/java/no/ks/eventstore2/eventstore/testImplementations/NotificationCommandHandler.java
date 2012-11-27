package no.ks.eventstore2.eventstore.testImplementations;

import akka.actor.ActorRef;
import no.ks.eventstore2.command.CommandHandler;
import no.ks.eventstore2.command.HandlesCommand;


@HandlesCommand(SendNotification.class)
public class NotificationCommandHandler extends CommandHandler {


    public NotificationCommandHandler(ActorRef eventStore) {
        super(eventStore);
    }

    public void handleCommand(SendNotification command) throws Exception {
        eventStore.tell(new NotificationSendt(), self());

    }
}
