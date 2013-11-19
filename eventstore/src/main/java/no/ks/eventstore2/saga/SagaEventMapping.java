package no.ks.eventstore2.saga;

import java.lang.reflect.Method;

public class SagaEventMapping {
    private final Class<? extends Saga> sagaClass;
    private final Method propertyMethod;

    public SagaEventMapping(Class<? extends Saga> sagaClass, Method propertyMethod) {
        this.sagaClass = sagaClass;
        this.propertyMethod = propertyMethod;
    }

    public Class<? extends Saga> getSagaClass() {
        return sagaClass;
    }

    public Method getPropertyMethod() {
        return propertyMethod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SagaEventMapping that = (SagaEventMapping) o;

        if (propertyMethod != null ? !propertyMethod.equals(that.propertyMethod) : that.propertyMethod != null)
            return false;
        if (sagaClass != null ? !sagaClass.equals(that.sagaClass) : that.sagaClass != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sagaClass != null ? sagaClass.hashCode() : 0;
        result = 31 * result + (propertyMethod != null ? propertyMethod.hashCode() : 0);
        return result;
    }
}
