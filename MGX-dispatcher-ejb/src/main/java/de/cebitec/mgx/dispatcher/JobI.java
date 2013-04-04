package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;


/**
 *
 * @author sjaenick
 */
public abstract class JobI implements Runnable {

    public JobI(Dispatcher d) {
        dispatcher = d;
    }
    private final Dispatcher dispatcher;

    public abstract void prepare();

    public abstract void process();

    public abstract void finished();

    public abstract void failed();

    public abstract void delete();

    public abstract JobState getState();

    public abstract void setState(JobState newState);

    public abstract String getProjectName();

    public abstract long getProjectJobID();
    
    public abstract String getConveyorGraph();

    @Override
    public void run() {
        JobState state = getState();
        if (state == JobState.IN_DELETION) {
            delete();
        } else {
            process();
        }

        dispatcher.handleExitingJob(this);
    }
    private int queueID = -1;

    public void setQueueID(int qId) {
        queueID = qId;
    }

    public int getQueueID() {
        return queueID;
    }
    private int priority = 500;

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}
