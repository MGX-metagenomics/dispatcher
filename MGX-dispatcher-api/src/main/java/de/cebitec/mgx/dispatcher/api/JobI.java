package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.common.JobState;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
public abstract class JobI implements Runnable {

    public static final int DEFAULT_PRIORITY = 500;

    public JobI(DispatcherI d, long projJobId, String projName, int prio) {
        dispatcher = d;
        projectJobId = projJobId;
        projectName = projName;
        priority = prio;
    }

    private final DispatcherI dispatcher;
    private final int priority;
    private final long projectJobId;
    private final String projectName;

    public abstract boolean validate() throws JobException;

    public abstract void prepare();

    public abstract void process();

    public abstract void finished();

    public abstract void failed();

    public abstract void delete();

    public abstract JobState getState() throws JobException;

    public abstract void setState(JobState newState) throws JobException;

    public String getProjectName() {
        return projectName;
    }

    public long getProjectJobID() {
        return projectJobId;
    }

    //public abstract String getConveyorGraph();
    public abstract String getProjectClass();

    @Override
    public void run() {
        try {
            JobState state = getState();
            if (state == JobState.IN_DELETION) {
                delete();
            } else {
                process();
            }
        } catch (JobException ex) {
            Logger.getLogger(JobI.class.getName()).log(Level.SEVERE, null, ex);
        }
        dispatcher.handleExitingJob(this);
    }
    //private int queueID = -1;

//    public void setQueueID(int qId) {
//        queueID = qId;
//    }
//
//    public int getQueueID() {
//        return queueID;
//    }
    public int getPriority() {
        return priority;
    }

//    public void setPriority(int priority) {
//        this.priority = priority;
//    }
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + this.getProjectName().hashCode();
        hash = (int) (17 * hash + this.getProjectJobID());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JobI other = (JobI) obj;
        if (this.getProjectJobID() != other.getProjectJobID()) {
            return false;
        }
        if (!this.getProjectClass().equals(other.getProjectClass())) {
            return false;
        }
        if (!this.getProjectName().equals(other.getProjectName())) {
            return false;
        }
        return true;
    }

}
