package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.JobException;
import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.api.DispatcherConfigurationI;
import de.cebitec.mgx.dispatcher.api.JobQueueI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.Schedule;

/**
 *
 * @author sjaenick
 */
@Singleton(mappedName = "Dispatcher")
@Startup
public class Dispatcher implements DispatcherI {

    @EJB
    private DispatcherConfigurationI config;
    @EJB
    private JobQueueI queue;

    private ThreadPoolExecutor tp = null;
    private final static Logger logger = Logger.getLogger(Dispatcher.class.getPackage().getName());
    private final Map<JobI, Future<?>> activeJobs = new HashMap<>();
    private boolean queueMode = false;

    @PostConstruct
    public void init() {
        log("Starting MGX dispatcher");
        int queueSize = queue.size();
        log("%d jobs in queue, execution limited to max %d parallel jobs", queueSize, config.getMaxJobs());
        tp = ThreadPoolExecutorFactory.createPool(config);

        // we cannot perform an initial scheduling run here because job factories
        // are not yet registered with the factory holder.
    }

    @PreDestroy
    public void destroy() {
        log("Stopping MGX dispatcher");
        queueMode = true;
        // save unprocessed jobs back to queue
        int cnt = 0;
        try {
            for (Runnable r : tp.shutdownNow()) {
                JobI job = (JobI) r;
                job.setState(JobState.QUEUED);
                queue.createJob(job);
                cnt++;
            }
        } catch (MGXDispatcherException | JobException ex) {
            log(ex.getMessage());
        }
        log(cnt + " jobs saved to queue");
    }

    @Override
    public boolean createJob(JobI job) throws MGXDispatcherException {
        queue.createJob(job);
        try {
            job.setState(JobState.QUEUED);
        } catch (JobException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex);
        }

        // attempt to schedule a job from the queue
        scheduleJobs();

        return true;
    }

    @Override
    public void cancelJob(JobI job) throws MGXDispatcherException {
        // try to remove job from queue
        boolean deleted = queue.deleteJob(job);

        if (!deleted) {
            // abort job if already running
            if (activeJobs.containsKey(job)) {
                Future<?> f = activeJobs.remove(job);
                if (f != null) {
                    deleted = f.cancel(true);
                    if (deleted) {
                        log("Job " + job.getProjectJobID() + " (" + job.getProjectName() + ") aborted.");
                    }
                }
            }
        }
    }

    @Override
    public void deleteJob(JobI job) throws MGXDispatcherException {
        // job might be queued or running, so we cancel it, just in case
        cancelJob(job);
        scheduleJobs();
    }

    @Override
    public void handleExitingJob(JobI job) {
        if (activeJobs.containsKey(job)) {
            activeJobs.remove(job);
        }
    }

    @Schedule(hour = "*", minute = "*", second = "0", persistent = false)
    @Override
    public synchronized void scheduleJobs() {

        if (queueMode) {
            log("QUEUEING MODE, %d jobs queued, %d jobs running.", queue.size(), tp.getActiveCount());
            return;
        }

        if (queue.size() == 0 || tp.getActiveCount() > 0) {
            log("Queue is empty, %d jobs running.", tp.getActiveCount());
            return;
        }

        while ((!tp.isTerminating()) && (queue.size() > 0)) {
            if (tp.getActiveCount() < config.getMaxJobs()) {
                JobI job = null;
                try {
                    job = queue.nextJob();
                } catch (MGXDispatcherException ex) {
                    Logger.getLogger(Dispatcher.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (job == null) {
                    return;
                }

                JobState state = null;
                try {
                    state = job.getState();
                } catch (JobException ex) {
                    log(ex.getMessage());
                }

                if (state != null && state.equals(JobState.QUEUED)) {
                    log("Scheduling job %s/%d", job.getProjectName(), job.getProjectJobID());
                    Future<?> f = tp.submit(job);
                    activeJobs.put(job, f);
                } else {
                    log("Not scheduling job %d in project %s due to unexpected state %s", job.getProjectJobID(), job.getProjectName(), state.toString());
                    log("Override in place, scheduling (FIXME)...");
                    // OVERRIDE - state changes are broken?!?!
                    Future<?> f = tp.submit(job);
                    activeJobs.put(job, f);
                }
            } else {
                log("All slots busy, not scheduling additional jobs.");
                return;
            }
        }
    }

    @Override
    public void setQueueMode(boolean qMode) {
        queueMode = qMode;

        if (!queueMode) {
            log("Resuming job scheduling.");
            scheduleJobs();
        }
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }

    @Override
    public boolean validate(JobI job) {
        try {
            if (job.validate()) {
                job.setState(JobState.VERIFIED);
                return true;
            } else {
                job.setState(JobState.FAILED);
                return false;
            }
        } catch (JobException ex) {
            log(ex.getMessage());
        }
        return false;
    }
}
