package edu.bdav.ta.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

/**
 * Defaults loading plus range-checked, user-friendly accessors.
 *
 * <p>Every accessor fails with an {@link IllegalArgumentException} naming the
 * key, the offending value and the accepted range — this is what keeps stack
 * traces away from the user (see {@code JobBootstrap.launch}).</p>
 */
public final class ConfigSupport {

    /** Classpath resource holding shipped defaults. */
    public static final String DEFAULTS_RESOURCE = "defaults.properties";

    private ConfigSupport() {
    }

    /**
     * Creates a {@link Configuration} with project defaults applied.
     *
     * <p>Defaults are applied with {@link Configuration#set} only when the key is
     * not already present, so an explicit {@code -D} from
     * {@code GenericOptionsParser} (which lands in the configuration overlay)
     * always wins regardless of resource load order.</p>
     */
    public static Configuration withDefaults() {
        Configuration conf = new Configuration();
        applyDefaults(conf);
        return conf;
    }

    /** Applies {@code defaults.properties} to an existing configuration. */
    public static void applyDefaults(Configuration conf) {
        Properties defaults = loadDefaults();
        for (String name : defaults.stringPropertyNames()) {
            if (conf.get(name) == null) {
                conf.set(name, defaults.getProperty(name).trim());
            }
        }
    }

    /** Reads the shipped defaults; a missing resource is a packaging bug. */
    public static Properties loadDefaults() {
        Properties props = new Properties();
        ClassLoader loader = ConfigSupport.class.getClassLoader();
        try (InputStream in = loader.getResourceAsStream(DEFAULTS_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + DEFAULTS_RESOURCE);
            }
            props.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + DEFAULTS_RESOURCE + ": " + e.getMessage(), e);
        }
        return props;
    }

    /** Range-checked integer accessor. */
    public static int getInt(Configuration conf, String key, int min, int max) {
        String raw = conf.get(key);
        if (raw == null) {
            throw new IllegalArgumentException("missing required key " + key);
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("key " + key + " expects an integer but was '" + raw + "'");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "key " + key + " must be in [" + min + ", " + max + "] but was " + value);
        }
        return value;
    }

    /** Range-checked long accessor. */
    public static long getLong(Configuration conf, String key, long min, long max) {
        String raw = conf.get(key);
        if (raw == null) {
            throw new IllegalArgumentException("missing required key " + key);
        }
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("key " + key + " expects a long but was '" + raw + "'");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "key " + key + " must be in [" + min + ", " + max + "] but was " + value);
        }
        return value;
    }

    /** Boolean accessor accepting only true/false. */
    public static boolean getBoolean(Configuration conf, String key) {
        String raw = conf.get(key);
        if (raw == null) {
            throw new IllegalArgumentException("missing required key " + key);
        }
        String v = raw.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(v)) {
            return true;
        }
        if ("false".equals(v)) {
            return false;
        }
        throw new IllegalArgumentException("key " + key + " expects true/false but was '" + raw + "'");
    }

    /** Trimmed string accessor with a fallback; blank counts as absent. */
    public static String getTrimmed(Configuration conf, String key, String fallback) {
        String raw = conf.get(key);
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        return raw.trim();
    }

    /** Trimmed string accessor that rejects blanks - used for mandatory paths. */
    public static String getRequired(Configuration conf, String key) {
        String value = getTrimmed(conf, key, null);
        if (value == null) {
            throw new IllegalArgumentException(
                    "key " + key + " is required (e.g. -D" + key + "=/data/stopwords/en-stopwords.txt)");
        }
        return value;
    }

    /**
     * Parses a normalisation policy from the configuration.
     *
     * @return the enum constant matching {@link ConfKeys#NUMBER_MODE}
     */
    public static <E extends Enum<E>> E getEnum(Configuration conf, String key, Class<E> type, E fallback) {
        String raw = getTrimmed(conf, key, null);
        if (raw == null) {
            return fallback;
        }
        try {
            return Enum.valueOf(type, raw.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            StringBuilder accepted = new StringBuilder();
            for (E constant : type.getEnumConstants()) {
                if (accepted.length() > 0) {
                    accepted.append('|');
                }
                accepted.append(constant.name().toLowerCase(Locale.ROOT));
            }
            throw new IllegalArgumentException(
                    "key " + key + " expects " + accepted + " but was '" + raw + "'");
        }
    }
}
