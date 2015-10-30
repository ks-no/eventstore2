package no.ks.eventstore2.util;

import java.util.UUID;

public class IdUtil {

	private IdUtil(){}
	public static String createUUID() {
		return UUID.randomUUID().toString();
	}
}
