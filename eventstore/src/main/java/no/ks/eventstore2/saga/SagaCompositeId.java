package no.ks.eventstore2.saga;

public class SagaCompositeId {

        private Class<?> clz;
        private String id;

        public SagaCompositeId(Class<?> clz, String id) {
            this.clz = clz;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SagaCompositeId that = (SagaCompositeId) o;

            if (clz != null ? !clz.equals(that.clz) : that.clz != null) return false;
            if (id != null ? !id.equals(that.id) : that.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = clz != null ? clz.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

    public Class<?> getClz() {
        return clz;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "SagaCompositeId{" +
                "clz=" + clz +
                ", id='" + id + '\'' +
                '}';
    }
}
