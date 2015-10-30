package no.ks.eventstore2.eventstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;


public class MongoDbOperations {

    private static final Logger log = LoggerFactory.getLogger(MongoDbOperations.class);

    private MongoDbOperations(){}

    public static <T> T doDbOperation(Callable<T> callable, int retries, int sleepms) {
        T result = null;
        boolean finished = false;
        int retrycount = 0;
        Exception exception = null;
        while (!finished && retrycount <= retries) {
            try {
                retrycount++;
                if (retrycount > 1)
                    log.info("MongDb got exception retrying operation", new RuntimeException("Retrying db operation"));
                result = callable.call();
                finished = true;
            } catch (Exception e) {
                log.info("failed db operation", e);
                exception = e;
                try {
                    Thread.sleep(sleepms);
                } catch (InterruptedException e1) {
                }
            }
        }
        if(exception != null) throw new RuntimeException(exception);
        return result;
    }

    public static <T> T doDbOperation(Callable<T> callable) {
        return doDbOperation(callable, 50, 500);
    }

}