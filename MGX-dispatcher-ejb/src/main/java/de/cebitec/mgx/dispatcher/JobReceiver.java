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
    FactoryHolder factories;

    public boolean submit(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            try {
                if (!job.getState().equals(JobState.SUBMITTED)) {
                    throw new MGXDispatcherException("Job is in invalid state " + job.getState());
                }
            } catch (JobException ex) {
                throw new MGXDispatcherException(ex);
            }
            return dispatcher.createJob(job);
        }
        return false;
    }

    public boolean validate(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            return dispatcher.validate(job);
        }
        return false;
    }

    public void delete(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            dispatcher.deleteJob(job);
        }
    }

    public void cancel(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            dispatcher.cancelJob(job);
        }
    }

    public boolean shutdown(UUID auth) {
        return dispatcher.shutdown(auth);
    }

    private JobI getJob(String projClass, String projName, long projectJobId) {
        JobFactoryI fact = factories.getFactory(projClass);
        if (fact != null) {
            return fact.createJob(projName, projectJobId);
        }
        return null;
    }
}
