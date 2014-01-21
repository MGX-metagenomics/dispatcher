package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private HashMap<Long, Future<?>> activeJobs;
    private boolean queueMode;

    @PostConstruct
    public void init() {
        log("Starting MGX dispatcher");
        activeJobs = new HashMap<>();
        int queueSize = queue.size();
        queueMode = false;
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
        queue.removeJob(job);

        if (activeJobs.containsKey(job.getProjectJobID())) {
            Future<?> f = activeJobs.get(job.getProjectJobID());
            f.cancel(true);
            activeJobs.remove(job.getProjectJobID());
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
        if (activeJobs.containsKey(job.getProjectJobID())) {
            activeJobs.remove(job.getProjectJobID());
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
                        log("Scheduling job %d", job.getQueueID());
                        //tp.execute(job);
                        Future<?> f = tp.submit(job);
                        activeJobs.put(job.getProjectJobID(), f);
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
            if (validateParameters(job)) {
                job.setState(JobState.VERIFIED);
                return true;
            }
        } catch (MGXDispatcherException | JobException ex) {
            log(ex.getMessage());
        }

        return false;
    }

    private boolean validateParameters(JobI j) throws MGXDispatcherException {

        // build up command string
        List<String> commands = new ArrayList<>();
        commands.add(config.getValidatorExecutable());
        commands.add(j.getConveyorGraph());
        commands.add(j.getProjectName());
        commands.add(String.valueOf(j.getProjectJobID()));

        String[] argv = commands.toArray(new String[0]);

        StringBuilder output = new StringBuilder();
        Integer exitCode = null;
        try {
            Process p = Runtime.getRuntime().exec(argv);
            BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String s;
            while ((s = stdout.readLine()) != null) {
                output.append(s);
            }
            stdout.close();

            while (exitCode == null) {
                try {
                    exitCode = p.waitFor();
                } catch (InterruptedException ex) {
                }
            }
        } catch (IOException ex) {
            log(ex.getMessage());
        }

        if (exitCode != null && exitCode.intValue() == 0) {
            return true;
        }

        throw new MGXDispatcherException(output.toString());
    }
}
