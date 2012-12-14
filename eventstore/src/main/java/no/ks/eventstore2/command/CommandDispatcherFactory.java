package no.ks.eventstore2.command;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

import java.util.List;

public class CommandDispatcherFactory implements UntypedActorFactory {
	private static final long serialVersionUID = 1L;

	private List<CommandHandlerFactory> commandHandlerFactories;
    private ActorRef eventStore;

    public CommandDispatcherFactory(List<CommandHandlerFactory> commandHandlerFactories, ActorRef eventStore) {
        this.commandHandlerFactories = commandHandlerFactories;
        this.eventStore = eventStore;
    }

    public CommandDispatcherFactory() {
    }

    public Actor create() throws Exception {
        return new CommandDispatcher(eventStore, commandHandlerFactories);
    }

    public void setCommandHandlerFactories(List<CommandHandlerFactory> commandHandlerFactories) {
        this.commandHandlerFactories = commandHandlerFactories;
    }

    public void setEventStore(ActorRef eventStore) {
        this.eventStore = eventStore;
    }
}
