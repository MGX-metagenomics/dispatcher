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
    private Dispatcher dispatcher;
    @EJB
    private FactoryHolder factories;

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
        System.err.println("got cancel req for job id "+ projectJobId);
        JobI job = getJob(projClass, projName, projectJobId);
        if (job == null) {
            throw new MGXDispatcherException("No job with ID " + projectJobId + " found in project " + projName);
        }
        dispatcher.cancelJob(job);
    }

    public boolean shutdown(UUID auth) {
        return dispatcher.shutdown(auth);
    }

    private JobI getJob(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobFactoryI fact = factories.getFactory(projClass);
        if (fact == null) {
            Logger.getLogger(JobReceiver.class.getName()).log(Level.SEVERE, null, "No registered job factory for project class "+ projClass);
            throw new MGXDispatcherException("No registered job factory for project class "+ projClass);
        }
        return fact.createJob(projName, projectJobId);
    }
}
