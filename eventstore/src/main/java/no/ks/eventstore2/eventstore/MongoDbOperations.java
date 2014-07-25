package no.ks.eventstore2.eventstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;


public class MongoDbOperations {

    private static final Logger log = LoggerFactory.getLogger(MongoDbOperations.class);

    public static <T> T doDbOperation(Callable<T> callable) {
        T result = null;
        boolean finished = false;
        int retrycount = 0;
        while (!finished && retrycount < 100) {
            try {
                retrycount++;
                if (retrycount > 1)
                    log.info("MongDb got exception retrying operation", new RuntimeException("Retrying db operation"));
                result = callable.call();
                finished = true;
            } catch (Exception e) {
                log.debug("failed db operation", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                }
            }
        }
        return result;
    }
}