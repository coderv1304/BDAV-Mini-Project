package edu.bdav.ta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import edu.bdav.ta.config.ConfigSupport;

/**
 * Test doubles for Hadoop's task contexts.
 *
 * <p><b>Why mocks instead of MRUnit.</b> MRUnit is retired (Apache Attic; last
 * release 1.1.0, 2014) and does not support the {@code mapreduce} API used here.
 * The replacement is a plain JUnit test that calls {@code map}/{@code reduce}
 * directly with a mocked context - which is also more precise, because it lets a
 * test assert on counters, not just on emitted records.</p>
 *
 * <p>The mocked context keeps a real {@link Counters} behind
 * {@code getCounter}, so counter assertions test real Hadoop counter semantics
 * rather than a stub's opinion.</p>
 *
 * <p><b>Emissions are deep-copied.</b> Production code reuses key and value objects
 * (that is the whole point of the object-reuse requirement), so capturing the
 * reference would record the <em>final</em> state of every object. Copying through
 * {@code Writable.write}/{@code readFields} captures exactly what the framework would
 * have serialised.</p>
 */
public final class TestSupport {

    private TestSupport() {
    }

    /** One emitted (key, value) pair captured from a mapper or reducer. */
    public static final class Emitted {
        private final Object key;
        private final Object value;

        Emitted(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        /** @return the emitted key, as it was at write time (copy it if mutable) */
        public Object key() {
            return key;
        }

        /** @return the emitted value, as it was at write time */
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    /** Builds a configuration with project defaults plus overrides. */
    public static Configuration conf(String... keyValues) {
        Configuration conf = ConfigSupport.withDefaults();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            conf.set(keyValues[i], keyValues[i + 1]);
        }
        return conf;
    }

    /**
     * Creates a mapper context that records emissions.
     *
     * @param sink destination for emitted pairs
     * @param counters counter registry backing {@code getCounter}
     * @param conf configuration returned by {@code getConfiguration}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Mapper.Context mapperContext(final List<Emitted> sink, final Counters counters,
                                               final Configuration conf) {
        final Mapper.Context context = mock(Mapper.Context.class);
        Answer<Void> capture = invocation -> {
            sink.add(new Emitted(copy(invocation.getArgument(0)), copy(invocation.getArgument(1))));
            return null;
        };
        try {
            Mockito.doAnswer(capture).when(context).write(any(), any());
            when(context.getCounter(any(Enum.class)))
                    .thenAnswer(invocation -> counters.findCounter((Enum) invocation.getArgument(0)));
            when(context.getCounter(org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.anyString()))
                    .thenAnswer(invocation -> counters.findCounter((String) invocation.getArgument(0),
                            (String) invocation.getArgument(1)));
            when(context.getConfiguration()).thenReturn(conf);
        } catch (Exception e) {
            throw new IllegalStateException("cannot build mapper context", e);
        }
        return context;
    }

    /** Creates a reducer context that records emissions. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Reducer.Context reducerContext(final List<Emitted> sink, final Counters counters,
                                                 final Configuration conf) {
        final Reducer.Context context = mock(Reducer.Context.class);
        Answer<Void> capture = invocation -> {
            sink.add(new Emitted(copy(invocation.getArgument(0)), copy(invocation.getArgument(1))));
            return null;
        };
        try {
            Mockito.doAnswer(capture).when(context).write(any(), any());
            when(context.getCounter(any(Enum.class)))
                    .thenAnswer(invocation -> counters.findCounter((Enum) invocation.getArgument(0)));
            when(context.getCounter(org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.anyString()))
                    .thenAnswer(invocation -> counters.findCounter((String) invocation.getArgument(0),
                            (String) invocation.getArgument(1)));
            when(context.getConfiguration()).thenReturn(conf);
        } catch (Exception e) {
            throw new IllegalStateException("cannot build reducer context", e);
        }
        return context;
    }

    /**
     * Deep-copies a Writable through its own serialisation, so the captured value is
     * the serialised record rather than a live object the caller keeps mutating.
     */
    static Object copy(Object value) {
        if (!(value instanceof Writable)) {
            return value;
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                ((Writable) value).write(out);
            }
            Writable clone = (Writable) value.getClass().getDeclaredConstructor().newInstance();
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                clone.readFields(in);
            }
            return clone;
        } catch (Exception e) {
            throw new IllegalStateException("cannot copy " + value.getClass().getName(), e);
        }
    }

    /** Convenience: fresh emission sink. */
    public static List<Emitted> sink() {
        return new ArrayList<Emitted>();
    }

    /** Renders a sink as {@code key=value} strings, in emission order. */
    public static List<String> render(List<Emitted> emissions) {
        List<String> rendered = new ArrayList<>(emissions.size());
        for (Emitted emitted : emissions) {
            rendered.add(emitted.key() + "=" + emitted.value());
        }
        return rendered;
    }

    /** Reads one counter value from a registry. */
    public static long counter(Counters counters, Enum<?> name) {
        return counters.findCounter(name).getValue();
    }

    /** Returns a map entry list size helper for readability in assertions. */
    public static int size(Map<?, ?> map) {
        return map.size();
    }
}
