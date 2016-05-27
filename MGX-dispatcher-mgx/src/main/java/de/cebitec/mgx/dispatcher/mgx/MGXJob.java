package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.JobException;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import de.cebitec.mgx.dispatcher.mgx.MGXJobFactory.ConnectionProviderI;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
public class MGXJob extends JobI {

    // project-specific job id
    private final String conveyorGraph;
    private final String persistentDir;
    private final String conveyorValidate;
    private final String conveyorExecutable;
    private final ConnectionProviderI cc;
    private final Executor executor;
    private final static Logger logger = Logger.getLogger(MGXJob.class.getPackage().getName());

    public MGXJob(Dispatcher disp, Executor executor, String conveyorExec, String conveyorValidate, String persistentDir,
            ConnectionProviderI cc, String projName,
            long mgxJobId) throws MGXDispatcherException {

        super(disp, mgxJobId, projName, JobI.DEFAULT_PRIORITY);
        this.conveyorValidate = conveyorValidate;
        this.conveyorExecutable = conveyorExec;
        this.persistentDir = persistentDir;
        this.cc = cc;
        this.executor = executor;
        conveyorGraph = lookupGraphFile(mgxJobId);
    }

    @Override
    public void prepare() {
        //
    }

    @Override
    public void process() {
        // build up command string
        List<String> commands = new ArrayList<>(4);
        commands.add(conveyorExecutable);
        commands.add(conveyorGraph);
        commands.add(getProjectName());
        commands.add(String.valueOf(getProjectJobID()));

        try {
            setState(JobState.RUNNING);
            setStartDate();
        } catch (JobException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            try {
                setState(JobState.FAILED);
            } catch (JobException ex1) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
            return;
        }

        Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", join(commands, " "));

        try {

            Process p = null;
            int exitCode = -1;
            try {
                Runtime r = Runtime.getRuntime();
                if (r == null) {
                    log("Could not obtain runtime.");
                    setState(JobState.ABORTED);
                    setFinishDate();
                    return;
                }
                p = r.exec(commands.toArray(new String[]{}));
                if (p == null) {
                    log("Could not execute command: " + join(commands, " "));
                    setState(JobState.ABORTED);
                    setFinishDate();
                    return;
                }

                exitCode = p.waitFor();
            } catch (InterruptedException ex) {
                /*
                 * job was aborted
                 */
                setState(JobState.ABORTED);
                setFinishDate();
                return;
            } catch (IOException ex) {
                // does this happen at all? under which conditions? log exception and
                // treat like job was cancelled, for now..
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
                setState(JobState.ABORTED);
                setFinishDate();
                return;
            } finally {
                if (p != null) {
                    p.destroy();
                }
            }

            if (exitCode == 0) {
                finished();
            } else {
                setState(JobState.FAILED);
            }
            setFinishDate();

        } catch (JobException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void failed() {
        try (Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

            // remove observations
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            // remove observations
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            // remove attributes
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM attribute WHERE job_id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            /*
             * we can't delete orphan attributetypes, since there might be other
             * analysis jobs running that rely on them; there's a short period
             * of time between attributetype creation and referencing the
             * attributetype in the attribute table
             */
            // mark job failed
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=? WHERE id=?")) {
                stmt.setLong(1, JobState.FAILED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
            }

            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void delete() {
        try (Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

            // remove observations
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            // remove attribute counts
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            // remove attributes
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM attribute WHERE job_id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            /*
             * we can't delete orphan attributetypes, since there might be other
             * analysis jobs running that rely on them; there's a short period
             * of time between attributetype creation and referencing the
             * attributetype in the attribute table
             */
            // delete the job
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM job WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            conn.commit();
            conn.setAutoCommit(true);
            conn.close();
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

//    @Override
//    public String getConveyorGraph() {
//        return conveyorGraph;
//    }
    private void setStartDate() throws JobException {
        int numRows = 0;
        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET startdate=NOW() WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                numRows = stmt.executeUpdate();
                stmt.close();
            }
            conn.close();
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }

        if (numRows != 1) {
            throw new JobException("Could not set start date");
        }
    }

    private void setFinishDate() throws JobException {
        int numRows = 0;
        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                numRows = stmt.executeUpdate();
                stmt.close();
            }
            conn.close();
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }
        if (numRows != 1) {
            throw new JobException("Could not set finish date");
        }
    }

    @Override
    public synchronized void setState(JobState state) throws JobException {
        //Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "{0}/{1}: state change {2} to {3}", new Object[]{projectName, mgxJobId, getState(), state});
        String sql = "UPDATE job SET job_state=? WHERE id=? RETURNING job_state";
        try (Connection conn = getProjectConnection()) {
            //conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            // acquire row lock
            try (PreparedStatement stmt = conn.prepareStatement("SELECT job_state FROM job where id=? FOR UPDATE")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, state.ordinal());
                stmt.setLong(2, getProjectJobID());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        JobState newState = JobState.values()[rs.getInt(1)];
                        if (newState != state) {
                            throw new JobException("DB update failed, expected " + state + ", got " + newState);
                        }
                    }
                    rs.close();
                }
                stmt.close();
            }
            conn.commit();
            conn.setAutoCommit(true);
            conn.close();
        } catch (Exception ex) {
            throw new JobException(ex);
        }

        JobState dbState = getState();
        if (dbState != state) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "DB inconsistent, expected {0}, got {1}", new Object[]{state, dbState});
            throw new JobException("DB inconsistent, expected " + state + ", got " + dbState);
        }
    }

    @Override
    public JobState getState() throws JobException {
        String sql = "SELECT job_state FROM job WHERE id=?";
        int state = -1;

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, getProjectJobID());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        state = rs.getInt(1);
                    }
                    rs.close();
                }
                stmt.close();
            }
            conn.close();
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex);
        }
        return JobState.values()[state];
    }

    private String lookupGraphFile(long jobId) throws MGXDispatcherException {
        String file = null;
        String sql = "SELECT Tool.xml_file FROM Job LEFT JOIN Tool ON (Job.tool_id=Tool.id) WHERE Job.id=?";

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, jobId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        file = rs.getString(1);
                    }
                    rs.close();
                }
                stmt.close();
            }
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }

        return file;
    }

    @Override
    public void finished() {
        try {
            setState(JobState.FINISHED);
        } catch (JobException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        try (Connection conn = getProjectConnection()) {
            // set the job to finished state
            conn.setAutoCommit(false);
            // create assignment counts for attributes belonging to this job
            String sql = "INSERT INTO attributecount "
                    + "SELECT attribute.id, count(attribute.id) FROM attribute "
                    + "LEFT JOIN observation ON (attribute.id = observation.attr_id) "
                    + "WHERE job_id=? GROUP BY attribute.id ORDER BY attribute.id";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            conn.commit();
            conn.setAutoCommit(true);
            conn.close();
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            try {
                setState(JobState.FAILED);
            } catch (JobException ex1) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        /*
         * remove stdout/stderr files for finished jobs; keep
         * them for debugging purposes, otherwise
         */
        StringBuilder sb = new StringBuilder()
                .append(persistentDir)
                .append(File.separator)
                .append(getProjectName())
                .append(File.separator)
                .append("jobs")
                .append(File.separator)
                .append(String.valueOf(getProjectJobID()))
                .append(".");
        File stdout = new File(sb.toString() + "stdout");
        if (stdout.exists()) {
            stdout.delete();
        }
        File stderr = new File(sb.toString() + "stderr");
        if (stderr.exists()) {
            stderr.delete();
        }
    }

    private Connection getProjectConnection() throws MGXDispatcherException {
        return cc.getProjectConnection(getProjectName());
    }

    protected void close(Statement s, ResultSet r) {
        try {
            if (r != null) {
                r.close();
            }
            if (s != null) {
                s.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String getProjectClass() {
        return "MGX";
    }

    @Override
    public boolean validate() throws JobException {

        File validate = new File(conveyorValidate);
        if (!validate.canRead() && validate.canExecute()) {
            throw new JobException("Unable to access Conveyor executable.");
        }

        File graph = new File(conveyorGraph);
        if (!graph.canRead()) {
            throw new JobException("Cannot read workflow file");
        }

        // build up command string
        List<String> commands = new ArrayList<>();
        commands.add(conveyorValidate);
        commands.add(conveyorGraph);
        commands.add(getProjectName());
        commands.add(String.valueOf(getProjectJobID()));

        String[] argv = commands.toArray(new String[]{});

        Integer exitCode = null;

        Process p = null;
        StreamLogger stdout, stderr = null;
        try {
            Runtime r = Runtime.getRuntime();
            if (r == null) {
                log("Could not obtain runtime.");
                return false;
            }
            p = r.exec(argv);
            if (p == null) {
                log("Could not execute command: " + join(commands, " "));
                return false;
            }

            stdout = new StreamLogger(p.getInputStream());
            stderr = new StreamLogger(p.getErrorStream());
            executor.execute(stdout);
            executor.execute(stderr);

            while (exitCode == null) {
                try {
                    exitCode = p.waitFor();
                } catch (InterruptedException ex) {
                }
            }
        } catch (IOException ex) {
            log(ex.getMessage());
        } finally {
            exiting = true;
            if (p != null) {
                try {
                    p.getInputStream().close();
                    p.getErrorStream().close();
                } catch (IOException ex) {
                    Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
                }
                p.destroy();
            }
        }

        if (exitCode != null && exitCode == 0) {
            return true;
        } else {
            log("Validation failed with exit code " + exitCode + ", commmand was " + join(commands, " "));
        }

        String output = stderr != null ? stderr.getOutput() : "";

        throw new JobException(output.length() > 0 ? output : "Unknown internal error.");
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }

    private static String join(final Iterable< ? extends Object> pColl, String separator) {
        Iterator< ? extends Object> oIter;
        if (pColl == null || (!(oIter = pColl.iterator()).hasNext())) {
            return "";
        }
        StringBuilder oBuilder = new StringBuilder(String.valueOf(oIter.next()));
        while (oIter.hasNext()) {
            oBuilder.append(separator).append(oIter.next());
        }
        return oBuilder.toString();
    }

    private volatile boolean exiting = false;

    private class StreamLogger implements Runnable {

        private final InputStream is;
        private final StringBuilder output = new StringBuilder();

        StreamLogger(InputStream in) {
            is = in;
        }

        public String getOutput() {
            return output.toString();
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while (!exiting && ((line = in.readLine()) != null)) {
                    if (!line.trim().isEmpty()) {
                        output.append(line);
                        output.append(System.lineSeparator());
                    }
                }
            } catch (IOException ex) {
                if (!exiting) {
                    Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
}
