package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class JobReceiver {

    @EJB
    protected Dispatcher dispatcher;
    @EJB
    private DispatcherConfiguration config;

    public boolean submit(String projName, long mgxJobId) throws MGXDispatcherException {
        JobI job = new MGXJob(dispatcher, config, projName, mgxJobId);
        try {
            if (!job.getState().equals(JobState.SUBMITTED)) {
                throw new MGXDispatcherException("Job is in invalid state "+ job.getState());
            }
        } catch (JobException ex) {
            throw new MGXDispatcherException(ex);
        }
        return dispatcher.createJob(job);
    }

    public boolean validate(String projName, long jobId) throws MGXDispatcherException {
        JobI job = new MGXJob(dispatcher, config, projName, jobId);
        return dispatcher.validate(job);
    }

    public void delete(String projName, long jobId) throws MGXDispatcherException {
        JobI job = new MGXJob(dispatcher, config, projName, jobId);
        dispatcher.deleteJob(job);
    }

    public void cancel(String projName, long jobId) throws MGXDispatcherException {
        JobI job = new MGXJob(dispatcher, config, projName, jobId);
        dispatcher.cancelJob(job);
    }

    public boolean shutdown(UUID auth) {
        return dispatcher.shutdown(auth);
    }
}
