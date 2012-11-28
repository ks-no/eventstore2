package no.ks.eventstore2.projection;

import java.util.Arrays;

public class Call {
    private final String method;
    private final Object[] args;

    public Call(String method, Object[] args) {
        this.method = method;
        this.args = args;
    }

    public String getMethodName() {
        return method;
    }

    public Object[] getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return "Call{"
                + "method='" + method + '\''
                + ", args=" + (args == null ? null : Arrays.asList(args))
                + '}';
    }
}
