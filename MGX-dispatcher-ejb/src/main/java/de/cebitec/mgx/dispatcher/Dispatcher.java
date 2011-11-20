package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import de.cebitec.mgx.dto.dto.JobDTO.JobState;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton(mappedName="Dispatcher")
@Startup
public class Dispatcher {

    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/DispatcherConfiguration")
    private DispatcherConfiguration config;
    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/JobQueue")
    private JobQueue queue;
    private ThreadPoolExecutor tp = null;
    private final static Logger logger = Logger.getLogger(Dispatcher.class.getPackage().getName());
    private HashMap<Long, Future<?>> activeJobs;
    private boolean queueMode;

    @PostConstruct
    public void init() throws MGXDispatcherException {
        log("Starting MGX dispatcher");
        activeJobs = new HashMap<Long, Future<?>>();
        int queueSize = queue.NumEntries();
        queueMode = false;
        log("%d jobs in queue, execution limited to max %d parallel jobs", queueSize, config.getMaxJobs());
        tp = ThreadPoolExecutorFactory.createPool(config);
        scheduleJobs();
    }

    @PreDestroy
    public void shutdown() {
        log("Stopping MGX dispatcher");
        queueMode = true;
        // save unprocessed jobs back to queue
        int cnt = 0;
        try {
            for (Runnable r : tp.shutdownNow()) {
                MGXJob j = (MGXJob) r;
                j.setState(JobState.PENDING);
                queue.createJob(j);
                cnt++;
            }
        } catch (MGXDispatcherException ex) {
            log(ex.getMessage());
        }
        log(cnt + " jobs saved to queue");
    }

    public void createJob(MGXJob job) throws MGXDispatcherException {
        queue.createJob(job);
        job.setState(JobState.PENDING);

        scheduleJobs();
    }

    public void cancelJob(MGXJob job) throws MGXDispatcherException {
        // try to remove job from queue
        queue.removeJob(job);

        if (activeJobs.containsKey(job.getMgxJobId())) {
            Future<?> f = activeJobs.get(job.getMgxJobId());
            f.cancel(true);
            activeJobs.remove(job.getMgxJobId());
        }
    }

    public void deleteJob(MGXJob job) throws MGXDispatcherException {
        // job might be queued or running, so we cancel it, just in case
        cancelJob(job);

        job.setState(JobState.IN_DELETION);
        queue.createJob(job);
        scheduleJobs();
    }

    public void handleExitingJob(MGXJob job) {
        if (activeJobs.containsKey(job.getMgxJobId())) {
            activeJobs.remove(job.getMgxJobId());
        }
        scheduleJobs();
    }

    private void scheduleJobs() {

        if (queueMode) {
            log("QUEUEING MODE, %d jobs queued, %d jobs running.", queue.NumEntries(), tp.getActiveCount());
            return;
        }

        while ((!tp.isTerminating()) && (queue.NumEntries() > 0)) {
            if (tp.getActiveCount() < config.getMaxJobs()) {
                MGXJob job = queue.nextJob();
                if (job != null) {
                    log("Scheduling job %d", job.getQueueId());
                    //tp.execute(job);
                    Future<?> f = tp.submit(job);
                    activeJobs.put(job.getMgxJobId(), f);
                }
            } else {
                log("All slots busy, not scheduling additional jobs.");
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

    public DispatcherConfiguration getConfig() {
        return config;
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }
}
