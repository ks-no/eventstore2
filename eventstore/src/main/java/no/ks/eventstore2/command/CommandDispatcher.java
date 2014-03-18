package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandDispatcher extends UntypedActor {

	private static Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

	private List<Props> commandHandlerProps;
    private ActorRef eventStore;
    private List<CommandHandlerFactory> commandHandlerFactories;
    private Map<Class<? extends Command>, ActorRef> commandHandlers = new HashMap<Class<? extends Command>, ActorRef>();

    private int remainingCommandHandlers = 0;

    public static Props mkProps(ActorRef eventStore, List<CommandHandlerFactory> commandHandlerFactories){
        return Props.create(CommandDispatcher.class, eventStore, commandHandlerFactories);
    }

    public static Props mkProps(List<Props> commandHandlerProps){
        return Props.create(CommandDispatcher.class, commandHandlerProps);
    }

    @Deprecated
    public CommandDispatcher(ActorRef eventStore, List<CommandHandlerFactory> commandHandlerFactories) {
		this.eventStore = eventStore;
		this.commandHandlerFactories = commandHandlerFactories;
        log.debug("CommandDispatcher created");
	}

    public CommandDispatcher(List<Props> commandHandlerProps) {
        this.commandHandlerProps = commandHandlerProps;
        log.debug("CommandDispatcher created");
    }

	@Override
	public void preStart() throws InterruptedException {
        log.debug("PreStartCalled");
        if(commandHandlerFactories != null) {
        	for (CommandHandlerFactory factory : commandHandlerFactories) {
        		factory.setEventStore(eventStore);
        		Props props = new Props(factory);
        		if (factory.useRouter()) {
        			props = props.withRouter(factory.getRouter());
        		}
        		ActorRef ref = getContext().actorOf(props);
        		log.debug("Created subactor " + ref);
        		ref.tell("HandlesClasses", self());
        		log.debug("sent Handles classes to " + ref);
        		remainingCommandHandlers++;
        	}
        }
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
	public void onReceive(Object o) throws Exception {

		if (o instanceof Command) {
            log.debug("Got command " + o);
			ActorRef actorRef = commandHandlers.get(o.getClass());
            if(actorRef == null && remainingCommandHandlers > 0) {
                log.debug("RemainingCommandHandlers is " + remainingCommandHandlers + " sending message to self");
                self().tell(o, sender());
            } else {
                if(actorRef == null){
                    log.error("Failed to find commandHandler for command {}", o);
                } else {
			        actorRef.tell(o, sender());
                }
            }
		} else if(o instanceof ImmutableSet){
            log.debug("Got ImmutableSet " + o);
            ImmutableSet<Class<? extends Command>> handles = (ImmutableSet<Class<? extends Command>>) o;
            for (Class<? extends Command> clz : handles) {
                log.debug("Putting class " + clz  + " into map with actor " + sender());
                commandHandlers.put(clz, sender());
                remainingCommandHandlers--;
                log.debug("remainingCommandHandlers is " + remainingCommandHandlers);
            }
        }
	}
}
