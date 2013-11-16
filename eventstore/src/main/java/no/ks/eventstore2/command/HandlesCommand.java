package no.ks.eventstore2.command;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
@Deprecated
public @interface HandlesCommand {
	Class<? extends Command>[] value();
}
