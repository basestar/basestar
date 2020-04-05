# Event interfaces

The relationship between interfaces is as follows:

- **Emitter**

  Publishes events, implementation must be provided (example SNS)

- **Receiver**

  Receives events, implementation must be provided (example SQS)

- **Handler**

  Implemented by services that wish to consume events

- **Pump**

  Pulls events from a receiver and passes them to a handler, a default multi-threaded implementation
  is provided in this module


The Pump and the Receiver may not be needed if for e.g. the SQS lambda connector is used,
in this case a handler would be passed to the connector and event processing would occur in the
lambda JVM.

## Serialization

Events require polymorphic ser/de, and deep type information must be preserved, Gzipped BSON is appropriate.

## Large/oversize events

Implementations should accommodate arbitrarily large events, typically using the Stash interface and passing
references to stashed values when implementation-specific limits are exceeded.

## Loopback

For tests and simple configurations, a Loopback Emitter + Receiver implementation is provided.


## Handler example

For convenient routing of events by type, the Handlers class is provided, usage is as follows:

```
public class MyHandler implements Handler<Event> {

    private static final Handlers HANDLERS = Handlers.builder()
        .on(MyEvent.class, this::onMyEvent)
        .build();

    @Override
    public CompletableFuture<?> handle(final Event event) {

        return HANDLERS.handle(this, event);
    }

    private CompletableFuture<?> onMyEvent(final MyEvent event) {

        // Handle the event
    }
}
```


