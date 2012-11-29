package no.ks.eventstore2.saga;

import no.ks.eventstore2.Event;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface EventIdBind {
	Class<? extends Event> eventClass();
	String idProperty();
}
