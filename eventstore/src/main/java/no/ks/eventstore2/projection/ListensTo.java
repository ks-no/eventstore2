package no.ks.eventstore2.projection;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Deprecated
@Retention(RetentionPolicy.RUNTIME)
public @interface ListensTo {
	Class[] value();

	String[] aggregates();
}
