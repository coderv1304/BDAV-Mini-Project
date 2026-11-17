package edu.bdav.ta.cli;

/**
 * Signals a user-correctable mistake (bad arguments, output already present).
 *
 * <p>Deliberately a {@link RuntimeException} that is <em>not</em> an
 * {@link IllegalArgumentException}, so {@code JobBootstrap.launch} can map it to
 * {@link ExitCodes#USAGE} distinctly from configuration errors. The message is
 * printed verbatim to the user; the stack trace is logged at DEBUG only.</p>
 */
public class UsageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** @param message user-facing explanation, no stack-trace detail */
    public UsageException(String message) {
        super(message);
    }

    /** @param message user-facing explanation @param cause internal detail (DEBUG only) */
    public UsageException(String message, Throwable cause) {
        super(message, cause);
    }
}
