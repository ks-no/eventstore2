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

Copyright 2014 KS
-----------------

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
