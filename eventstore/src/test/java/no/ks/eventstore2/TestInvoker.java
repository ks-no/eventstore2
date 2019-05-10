package no.ks.eventstore2;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;

public class TestInvoker {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private Duration interval = Duration.ofMillis(500);
    private Duration timeout = Duration.ofSeconds(10);


    public TestInvoker withInterval(Duration interval) {
        this.interval = interval;
        return this;
    }

    public TestInvoker withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public TestInvoker pre(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public <V> V invoke(Callable<V> callable) {
        Stopwatch elapsed = Stopwatch.createStarted();

        Throwable lastError = null;
        while (elapsed.elapsed().minus(timeout).isNegative()) {
            try {
                return callable.call();
            } catch (Throwable e){
                log.debug("Got error ({}), retrying in {} ms", e, interval.toMillis());
                lastError = e;
                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }

        if (lastError != null)
            throw new AssertionError(String.format("Test invocation timed out in %s. Last failure was:", timeout.toString()), lastError);
        else
            throw new AssertionError(String.format("Test invocation timed out in %s. No exception was caught", timeout.toString()));
    }

    public void invoke(Runnable runnable) {
        Stopwatch elapsed = Stopwatch.createStarted();

        Throwable lastError = null;
        while (elapsed.elapsed().minus(timeout).isNegative()) {
            try {
                runnable.run();
                return;
            } catch (Throwable e) {
                log.debug("Got error ({}), retrying in {} ms", e, interval.toMillis());
                lastError = e;
                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }

        if (lastError != null)
            throw new AssertionError(String.format("Test invocation timed out in %s. Last failure was:", timeout.toString()), lastError);
        else
            throw new AssertionError(String.format("Test invocation timed out in %s. No exception was caught", timeout.toString()));
    }

}
