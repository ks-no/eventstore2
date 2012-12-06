package akka.cluster;

import akka.actor.UntypedActor;
import no.ks.eventstore2.command.Command;

import java.util.ArrayList;
import java.util.List;

public class CommandDispatcher extends UntypedActor {


    public List<Command> commands = new ArrayList<Command>();

    /**
     * To be implemented by concrete UntypedActor, this defines the behavior of the
     * UntypedActor.
     */
    @Override
    public void onReceive(Object message) {

        if (message instanceof Command) {
            System.out.println("Got command " + message + " from " + sender().path());
            commands.add((Command) message);
        }
        if("list".equals(message)){
            System.out.println("Size is " + commands.size());
            sender().tell(commands.size());
        }
    }
}
