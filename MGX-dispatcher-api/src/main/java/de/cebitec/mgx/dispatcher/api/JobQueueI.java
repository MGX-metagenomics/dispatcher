
package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;

/**
 *
 * @author sj
 */
public interface JobQueueI {

    int createJob(JobI job) throws MGXDispatcherException;

    boolean deleteJob(JobI job);

    JobI nextJob() throws MGXDispatcherException;

    int size();
    
}
