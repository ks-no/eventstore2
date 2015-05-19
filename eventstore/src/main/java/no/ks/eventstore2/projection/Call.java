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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Call call = (Call) o;

        if (method != null ? !method.equals(call.method) : call.method != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(args, call.args);

    }

    @Override
    public int hashCode() {
        int result = method != null ? method.hashCode() : 0;
        result = 31 * result + (args != null ? Arrays.hashCode(args) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Call{"
                + "method='" + method + '\''
                + ", args=" + (args == null ? null : Arrays.asList(args))
                + '}';
    }
}
