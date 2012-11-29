package no.ks.eventstore2.saga;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ListensTo {
	EventIdBind[] value();

	String[] aggregates();
}
