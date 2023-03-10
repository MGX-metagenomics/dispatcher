package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.JobReceiverI;
import de.cebitec.mgx.dispatcher.api.JobException;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.api.FactoryHolderI;
import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class JobReceiver implements JobReceiverI {

    @EJB
    private DispatcherI dispatcher;
    @EJB
    private FactoryHolderI factories;

    @Override
    public boolean submit(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            try {
                if (!job.getState().equals(JobState.VERIFIED)) {
                    throw new MGXDispatcherException("Job is in invalid state " + job.getState());
                }
                job.setState(JobState.SUBMITTED);
            } catch (JobException ex) {
                throw new MGXDispatcherException(ex);
            }
            return dispatcher.createJob(job);
        }
        return false;
    }

    @Override
    public boolean validate(String projClass, String projName, long projectJobId) {
        JobI job = null;
        try {
            job = getJob(projClass, projName, projectJobId);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(JobReceiver.class.getName()).log(Level.SEVERE, ex.getMessage());
        }
        if (job != null) {
            return dispatcher.validate(job);
        }
        return false;
    }

    @Override
    public void delete(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job != null) {
            dispatcher.deleteJob(job);
        }
    }

    @Override
    public void cancel(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobI job = getJob(projClass, projName, projectJobId);
        if (job == null) {
            throw new MGXDispatcherException("No job with ID " + projectJobId + " found in project " + projName);
        }
        dispatcher.cancelJob(job);
    }

    @Override
    public boolean shutdown(UUID auth) {
        return dispatcher.shutdown(auth);
    }

    private JobI getJob(String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        return factories.createJob(dispatcher, projClass, projName, projectJobId);
    }
}
