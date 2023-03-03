package de.cebitec.mgx.dispatcher;

import java.io.Serial;

/**
 *
 * @author sjaenick
 */
public class JobException extends Exception {

    @Serial
    private static final long serialVersionUID = 6401253773779951803L;
    
    public JobException(String message) {
        super(message);
    }

    public JobException(Throwable cause) {
        super(cause);
    }

}
