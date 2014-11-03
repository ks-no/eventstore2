EventStore2
===========

EventStore2 is a framework for creating event sourced applications in Java. It features:

* Async execution and cluster support through Akka
* Annotation driven configuration of Projections, CommandHandlers and Sagas
* Json or binary (Kryo) serialization of persisted and networked events

### Maven dependency
```
<dependency>
  <groupId>no.ks</groupId>
  <artifactId>eventstore2</artifactId>
  <version>2.3.1</version>
</dependency>
```

Event Sourcing concepts
-----------------------

Event sourcing introduces a few concepts to your system:

* CommandDispatcher. Accepts commands (typically from controllers or sagas), and dispatches them to the apropriate command handler.
* CommandHandlers. Receives and validates commands, executes the required actions, and dispatchs events describing any state changes to the system.
* Events. Classes describing how the systems state has changed.
* Aggregates. Events are grouped in aggregates - collections of events that together form a entity where internal consistency is maintained.
* EventStore. Persistant stack of all events that have happened in the system.
* Projections. Objects that subscribe to a stream of events in the system, and provide a view of these to the application.
* SagaManager. Checks for existance of a saga with the specified id. If it exists it's retrieved from the repository, if not it's created.
* Sagas. Persisted objects that subscribe to a stream of events in the system, and alter internal state and dispatch commands based on these.

Using EventStore2
-----------------

EventStore2 starts one Akka actor pool per node (server) in your system. These nodes all share the same event store, and the system can withstand one or more nodes going down without service being interrupted. 

The library uses the following syntax to create the parts of an event sourced application. We'll use a simple blog application as an example:

### CommandHandler
```java
Class BlogPostHandler extends CommandHandler {
  
  public BlogPostHandler(ActorRef eventStore) {
    super(eventStore);
  }
  
  @Handler(CreateNewPost.class)
  public void handleCommand(CreateNewPost command){
    if (isAdmin(command.getUser())
      eventstore.tell(new NewPostCreated(command.getUser(), command.getContent);
    else
      throw new UnauthorizedActionException("User is not permitted to post to this blog");
  }
  
  ...

}
```
As all other components of our system, the command handler is an akka actor. The constructor accepts a reference to another actor, the EventStore component. 

The @Handler annotation tells the command dispatcher that this perticular command handler handles CreateNewPost commands, and that these commands should be dispatched to this actor.

On reception of CreateNewPost the handler checks if the user is allowed to complete the action, and then dispatches the NewPostCreated event to the event store to indicate that the systems state has changed.

### Projection
```java
@Subscriber("BlogAggregate")
Class Blog extends Projection {
  
  List<Post> posts = new ArrayList<>();
  
  public Blog(ActorRef eventStore) {
    super(eventStore);
  }
  
  @Handler(NewPostCreated.class)
  public void handleEvent(NewPostCreated event){
    posts.add(new Post(event.getCreated(), event.getUser(), event.getContent());   
  }
  
  public List<Post> getPosts(){
    return ImmutableList.copyOf(posts);
  }

}
```
The projections role is to maintain a view of our blog by updating a list of posts. Whenever a new post is created it is added to the list. The @Subscriber annotation specifies which aggregate the projection subscribes to.

If a call is made on the "getPosts" method a copy of the posts list will be return. It is good practice to return a copy instead of the list itself to maximise the self-contained nature of the akka actor representing the projection.



Copyright 2014 KS (MIT License)
-------------------------------

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
