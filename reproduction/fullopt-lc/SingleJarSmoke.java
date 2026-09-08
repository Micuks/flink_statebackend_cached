import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;

/** Local linkage/ownership check only; does not create a Flink cluster or job. */
class SingleJarSmoke {
    public static void main(String[] args) throws Exception {
        Path expected = new File(args[0]).getCanonicalFile().toPath();
        String[] names = {
            "org.apache.flink.table.data.binary.BinaryStringData",
            "org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader",
            "org.apache.flink.contrib.streaming.state.RocksDBValueState",
            "org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState",
            "org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState",
            "org.apache.flink.streaming.runtime.tasks.OneInputStreamTask",
            "org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction"
        };
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        for (String name : names) {
            Class<?> type = Class.forName(name, false, loader);
            Path actual = new File(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .getCanonicalFile().toPath();
            if (!expected.equals(actual)) {
                throw new AssertionError(name + " loaded from " + actual + " instead of " + expected);
            }
            System.out.println("ORIGIN_OK " + name + " " + actual);
        }
        Class<?> string = Class.forName(names[0], true, loader);
        Object original = string.getConstructor(String.class).newInstance("cachekit-\u4e2d\u6587");
        Object copied = string.getMethod("copy").invoke(original);
        Field binary = string.getSuperclass().getDeclaredField("binarySection");
        binary.setAccessible(true);
        boolean enabled = Boolean.parseBoolean(System.getProperty("cachekit.binary-string.lazy-copy.enabled"));
        if (original == copied || (binary.get(copied) == null) != enabled) {
            throw new AssertionError("LC copy semantics / gate mismatch");
        }
        if (!original.toString().equals(copied.toString())) {
            throw new AssertionError("value changed");
        }
        System.out.println("COPY_OK enabled=" + enabled);
    }
}
