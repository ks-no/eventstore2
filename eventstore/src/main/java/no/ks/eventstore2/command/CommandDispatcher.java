package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableSet;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static akka.pattern.Patterns.ask;
import static akka.testkit.JavaTestKit.duration;

public class CommandDispatcher extends UntypedActor {

	private ActorRef eventStore;
	private final List<CommandHandlerFactory> commandHandlerFactories;
	private Map<Class<? extends Command>, ActorRef> commandHandlers = new HashMap<Class<? extends Command>, ActorRef>();

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

			Future<Object> future = ask(ref, "HandlesClasses", 1000);
			try {
				ImmutableSet<Class<? extends Command>> handles = (ImmutableSet<Class<? extends Command>>) Await.result(future, duration("1 second"));
				for (Class<? extends Command> clz : handles) {
					commandHandlers.put(clz, ref);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void onReceive(Object o) throws Exception {
		if (o instanceof Command) {
			ActorRef actorRef = commandHandlers.get(o.getClass());
			actorRef.tell(o, sender());
		}
	}
}
