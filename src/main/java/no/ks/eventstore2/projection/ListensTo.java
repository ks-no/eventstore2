package no.ks.eventstore2.projection;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created with IntelliJ IDEA.
 * User: Torben Vesterager
 * Date: 08.08.12
 * Time: 12:40
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ListensTo {
	Class[] value();

	String[] aggregates();
}
