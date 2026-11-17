package edu.bdav.ta.cli;

/**
 * Process exit codes, so scripts can distinguish causes without parsing logs.
 *
 * <p>Verified by {@code scripts/05_run_all.sh}: only {@link #OK} means the stage
 * is usable, and {@link #USAGE} vs {@link #CONFIG} separates "wrong arguments"
 * from "wrong configuration value".</p>
 */
public final class ExitCodes {

    private ExitCodes() {
    }

    /** Stage completed and its output is usable. */
    public static final int OK = 0;
    /** Bad command line (missing arguments, bad flag). */
    public static final int USAGE = 2;
    /** Bad configuration value or missing mandatory key. */
    public static final int CONFIG = 3;
    /** MapReduce job failed after retries, or a required input was absent. */
    public static final int JOB_FAILED = 4;
    /** Filesystem / IO failure detected by the driver. */
    public static final int IO = 5;
    /** Output verification against the reference implementation failed. */
    public static final int VERIFY = 6;
}
