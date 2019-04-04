package no.ks.eventstore2.command;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandDispatcher extends AbstractActor {

	private static Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

	private List<Props> commandHandlerProps;
    private Map<Class<? extends Command>, ActorRef> commandHandlers = new HashMap<>();

    private int remainingCommandHandlers = 0;

    public static Props mkProps(List<Props> commandHandlerProps){
        return Props.create(CommandDispatcher.class, commandHandlerProps);
    }

    public CommandDispatcher(List<Props> commandHandlerProps) {
        this.commandHandlerProps = commandHandlerProps;
        log.debug("CommandDispatcher created");
    }

	@Override
	public void preStart() throws InterruptedException {
        log.debug("PreStartCalled");
        if(commandHandlerProps != null) {
        	for (Props prop : commandHandlerProps) {
        		ActorRef ref = getContext().actorOf(prop);
        		log.debug("Created subactor " + ref);
        		ref.tell("HandlesClasses", self());
        		log.debug("sent Handles classes to " + ref);
        		remainingCommandHandlers++;
        	}
        }
        //a small wait so that all children can start, before we start procesing messages.
        Thread.sleep(100);
	}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Command.class, this::handleCommand)
                .match(ImmutableSet.class, this::handleImmutableSet)
                .build();
    }

    private void handleCommand(Command command) {
        log.debug("Got command " + command);
        ActorRef actorRef = commandHandlers.get(command.getClass());
        if (actorRef == null && remainingCommandHandlers > 0) {
            log.debug("RemainingCommandHandlers is " + remainingCommandHandlers + " sending message to self");
            self().tell(command, sender());
        } else {
            if(actorRef == null){
                log.error("Failed to find commandHandler for command {}", command);
            } else {
                actorRef.tell(command, sender());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleImmutableSet(ImmutableSet set) {
        log.debug("Got ImmutableSet " + set);
        ImmutableSet<Class<? extends Command>> handles = (ImmutableSet<Class<? extends Command>>) set;
        for (Class<? extends Command> clz : handles) {
            log.debug("Putting class " + clz  + " into map with actor " + sender());
            commandHandlers.put(clz, sender());
            remainingCommandHandlers--;
            log.debug("remainingCommandHandlers is " + remainingCommandHandlers);
        }
    }
}
