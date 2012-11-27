package no.ks.eventstore2.eventstore.testImplementations;

import no.ks.eventstore2.command.CommandHandler;
import no.ks.eventstore2.command.HandlesCommand;


@HandlesCommand(SendNotification.class)
public class NotificationCommandHandler extends CommandHandler {


    public void handleCommand(SendNotification command) throws Exception {
        eventStore.tell(new NotificationSendt(), self());

    }
}
