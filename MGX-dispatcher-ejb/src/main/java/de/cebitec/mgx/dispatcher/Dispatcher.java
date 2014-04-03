package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton(mappedName = "Dispatcher")
@Startup
public class Dispatcher {

    @EJB
    private DispatcherConfiguration config;
    @EJB
    private JobQueue queue;
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
        scheduleJobs();
    }

    @PreDestroy
    public void destroy() {
        shutdown(config.getAuthToken());
    }

    public boolean shutdown(UUID auth) {
        if (!config.getAuthToken().equals(auth)) {
            log("Invalid authentication token.");
            return false;
        }
        log("Stopping MGX dispatcher");
        queueMode = true;
        // save unprocessed jobs back to queue
        int cnt = 0;
        try {
            for (Runnable r : tp.shutdownNow()) {
                JobI job = (JobI) r;
                job.setState(JobState.PENDING);
                queue.createJob(job);
                cnt++;
            }
        } catch (MGXDispatcherException | JobException ex) {
            log(ex.getMessage());
            return false;
        }
        log(cnt + " jobs saved to queue");
        return true;
    }

    public boolean createJob(JobI job) throws MGXDispatcherException {
        queue.createJob(job);
        try {
            job.setState(JobState.PENDING);
        } catch (JobException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex);
        }

        scheduleJobs();
        return true;
    }

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

    public void deleteJob(JobI job) throws MGXDispatcherException {
        // job might be queued or running, so we cancel it, just in case
        cancelJob(job);

//        job.setState(JobState.IN_DELETION);
//        queue.createJob(job);
        scheduleJobs();
    }

    public void handleExitingJob(JobI job) {
        if (activeJobs.containsKey(job)) {
            activeJobs.remove(job);
        }
        scheduleJobs();
    }

    private void scheduleJobs() {

        if (queueMode) {
            log("QUEUEING MODE, %d jobs queued, %d jobs running.", queue.size(), tp.getActiveCount());
            return;
        }

        while ((!tp.isTerminating()) && (queue.size() > 0)) {
            if (tp.getActiveCount() < config.getMaxJobs()) {
                JobI job = queue.nextJob();
                if (job != null) {
                    JobState state = null;
                    try {
                        state = job.getState();
                    } catch (JobException ex) {
                        log(ex.getMessage());
                    }

                    if (state != null && state.equals(JobState.PENDING)) {
                        log("Scheduling job %s/%d", job.getProjectName(), job.getProjectJobID());
                        Future<?> f = tp.submit(job);
                        activeJobs.put(job, f);
                    } else {
                        log("Not scheduling job %d due to unexpected state %s", job.getQueueID(), state.toString());
                    }
                }
            } else {
                log("All slots busy, not scheduling additional jobs.");
                return;
            }
        }
    }

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

    public boolean validate(JobI job) {
        try {
            if (job.validate()) {
                job.setState(JobState.VERIFIED);
                return true;
            }
        } catch (JobException ex) {
            log(ex.getMessage());
        }
        return false;
    }
}
