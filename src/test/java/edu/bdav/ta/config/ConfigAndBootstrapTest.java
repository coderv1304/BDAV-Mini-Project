package edu.bdav.ta.config;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.text.TextNormalizer;

/** Tests for the validated configuration layer and driver helpers. */
public class ConfigAndBootstrapTest {

    @Test
    public void appliesShippedDefaults() {
        Configuration conf = ConfigSupport.withDefaults();
        assertEquals("100", conf.get(ConfKeys.TOPN_N));
        assertEquals("mask", conf.get(ConfKeys.NUMBER_MODE));
        assertEquals("log", conf.get(ConfKeys.TF_MODE));
        assertTrue("output format must stay unset so each stage picks its own",
                conf.get(ConfKeys.OUTPUT_FORMAT) == null);
    }

    @Test
    public void explicitValuesWinOverDefaults() {
        Configuration conf = ConfigSupport.withDefaults();
        conf.set(ConfKeys.TOPN_N, "5");
        assertEquals(5, ConfigSupport.getInt(conf, ConfKeys.TOPN_N, 1, 1000));
    }

    @Test
    public void rejectsOutOfRangeIntegersWithAUsefulMessage() {
        Configuration conf = ConfigSupport.withDefaults();
        conf.set(ConfKeys.TOPN_N, "0");
        try {
            ConfigSupport.getInt(conf, ConfKeys.TOPN_N, 1, 1000);
            fail("expected a validation failure");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(ConfKeys.TOPN_N));
            assertTrue(expected.getMessage().contains("[1, 1000]"));
        }
    }

    @Test
    public void rejectsNonNumericValues() {
        Configuration conf = ConfigSupport.withDefaults();
        conf.set(ConfKeys.TOPN_N, "many");
        try {
            ConfigSupport.getInt(conf, ConfKeys.TOPN_N, 1, 1000);
            fail("expected a validation failure");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("expects an integer"));
        }
    }

    @Test
    public void parsesEnumsCaseInsensitivelyAndRejectsUnknownValues() {
        Configuration conf = ConfigSupport.withDefaults();
        conf.set(ConfKeys.NUMBER_MODE, "DROP");
        assertEquals(TextNormalizer.NumberMode.DROP,
                ConfigSupport.getEnum(conf, ConfKeys.NUMBER_MODE, TextNormalizer.NumberMode.class,
                        TextNormalizer.NumberMode.MASK));
        conf.set(ConfKeys.NUMBER_MODE, "explode");
        try {
            ConfigSupport.getEnum(conf, ConfKeys.NUMBER_MODE, TextNormalizer.NumberMode.class,
                    TextNormalizer.NumberMode.MASK);
            fail("expected a validation failure");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("keep|drop|mask"));
        }
    }

    @Test
    public void requiresMandatoryStrings() {
        Configuration conf = ConfigSupport.withDefaults();
        try {
            ConfigSupport.getRequired(conf, ConfKeys.STOPWORDS_PATH);
            fail("expected a validation failure for an empty required key");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(ConfKeys.STOPWORDS_PATH));
        }
    }

    @Test
    public void normalisesConvenienceFlags() {
        String[] normalised = JobBootstrap.normaliseArgs(new String[] {"--overwrite", "in", "--no-overwrite", "out"});
        assertArrayEquals(new String[] {"-D" + ConfKeys.OVERWRITE + "=true", "in",
                "-D" + ConfKeys.OVERWRITE + "=false", "out"}, normalised);
    }

    @Test
    public void mapsCodecNamesAndRejectsUnknownOnes() {
        assertEquals("org.apache.hadoop.io.compress.SnappyCodec",
                JobBootstrap.codecClass("snappy").getName());
        assertEquals("org.apache.hadoop.io.compress.Lz4Codec",
                JobBootstrap.codecClass("lz4").getName());
        try {
            JobBootstrap.codecClass("brotli");
            fail("expected an unknown codec to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("unknown codec"));
        }
    }

    @Test
    public void unwrapsTheRootCauseForUserFacingMessages() {
        Exception wrapped = new IllegalStateException("outer",
                new java.io.IOException("output directory not set"));
        assertTrue(JobBootstrap.describe(wrapped).contains("output directory not set"));
    }
}
