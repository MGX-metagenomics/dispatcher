package de.cebitec.mgx.dispatcher;

/**
 *
 * @author sjaenick
 */
public class JobException extends Exception {

    public JobException(String message) {
        super(message);
    }

    public JobException(Throwable cause) {
        super(cause);
    }

}
