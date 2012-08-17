package de.cebitec.mgx.dispatcher.common;

/**
 *
 * @author sjaenick
 */
public interface JobReceiverI {

    void submit(DispatcherCommand c, String projName, long jobId) throws MGXDispatcherException;
}
