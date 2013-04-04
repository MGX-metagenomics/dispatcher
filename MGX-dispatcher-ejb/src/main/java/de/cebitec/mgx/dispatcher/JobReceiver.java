package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.util.UUID;
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
        assert job.getState().equals(JobState.SUBMITTED);
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
