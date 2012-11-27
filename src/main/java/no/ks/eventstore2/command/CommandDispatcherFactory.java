package no.ks.eventstore2.command;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.UntypedActorFactory;

import java.util.List;

public class CommandDispatcherFactory implements UntypedActorFactory {


    private List<CommandHandlerFactory> commandHandlerFactories;
    private ActorRef eventStore;

    @Override
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
