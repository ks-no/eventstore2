
EventStore2
===========

EventStore2 is a framework for creating event sourced applications in Java. It features:

* Async execution and cluster support through Akka
* Annotation driven configuration of Projections, CommandHandlers and Sagas
* Json or binary (Kryo) serialization of persisted and networked events

Event Sourcing concepts
-----------------------

Event sourcing introduces a few concepts to your system:

* CommandDispatcher. Chooses the correct handler to execute an incoming command.
* CommandHandlers. Receives and validates commands, execute the required actions, and dispatch events describing any state changes to the system.
* Events. Classes describing how the systems state has changed.
* EventStore. Persistant stack of all events that have happened in the system.
* Projections. Objects that subscribe to a stream of events in the system, and provide a view of these to the application.
* SagaManager. Checks for existance of a saga with the specified id. If it exists it's retrieved from the repository, if not it's created.
* Sagas. Persisted objects that subscribe to a stream of events in the system, and alter internal state and dispatch commands based on these
