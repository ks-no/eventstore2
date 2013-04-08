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

	private ActorRef eventStore;
	private final List<CommandHandlerFactory> commandHandlerFactories;
	private Map<Class<? extends Command>, ActorRef> commandHandlers = new HashMap<Class<? extends Command>, ActorRef>();

    int remainingCommandHandlers = 0;
    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    public CommandDispatcher(ActorRef eventStore, List<CommandHandlerFactory> commandHandlerFactories) {
		this.eventStore = eventStore;
		this.commandHandlerFactories = commandHandlerFactories;
	}

	@Override
	public void preStart() {
		for (CommandHandlerFactory factory : commandHandlerFactories) {
			factory.setEventStore(eventStore);
			Props props = new Props(factory);
			if (factory.useRouter()) {
				props = props.withRouter(factory.getRouter());
			}
			ActorRef ref = getContext().actorOf(props);
            ref.tell("HandlesClasses", self());
            remainingCommandHandlers++;
		}
	}

	@Override
	public void onReceive(Object o) throws Exception {
		if (o instanceof Command) {
			ActorRef actorRef = commandHandlers.get(o.getClass());
            if(actorRef == null && remainingCommandHandlers > 0) {
                self().tell(o, sender());
            } else {
                if(actorRef == null){
                    log.error("Failed to find commandHandler for command {}", o);
                } else {
			        actorRef.tell(o, sender());
                }
            }
		} else if(o instanceof ImmutableSet){
            ImmutableSet<Class<? extends Command>> handles = (ImmutableSet<Class<? extends Command>>) o;
            for (Class<? extends Command> clz : handles) {
                commandHandlers.put(clz, sender());
                remainingCommandHandlers--;
            }
        }
	}
}
