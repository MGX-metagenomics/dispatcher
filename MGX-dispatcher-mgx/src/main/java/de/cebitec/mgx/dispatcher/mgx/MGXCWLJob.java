package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.JobException;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import de.cebitec.mgx.streamlogger.StringLogger;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
public class MGXCWLJob extends JobI {

    // project-specific job id
    private final String cwlTool;
    private final String workflow;
    private final String persistentDir;
    private final ConnectionProviderI cc;
    private final static Logger logger = Logger.getLogger(MGXCWLJob.class.getPackage().getName());

    public MGXCWLJob(Dispatcher disp,
            String cwlTool,
            String workflow,
            String persistentDir,
            ConnectionProviderI cc, String projName,
            long mgxJobId) throws MGXDispatcherException {

        super(disp, mgxJobId, projName, JobI.DEFAULT_PRIORITY);
        this.cwlTool = cwlTool;
        this.workflow = workflow;
        this.persistentDir = persistentDir;
        this.cc = cc;
    }

    @Override
    public void prepare() {
        //
    }

    @Override
    public void process() {

        String[] commands = new String[4];
        commands[0] = cwlTool;
        commands[1] = workflow;
        commands[2] = getProjectName();
        commands[3] = String.valueOf(getProjectJobID());

        try {
            setState(JobState.RUNNING);
            setStartDate();
        } catch (JobException ex) {
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
            failed();
            return;
        }

        Logger.getLogger(MGXCWLJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", join(commands, " "));

        Process p = null;

        try {
            ProcessBuilder pBuilder = new ProcessBuilder(commands);
            pBuilder.redirectErrorStream(true);

            p = pBuilder.start();
            if (p == null) {
                log("Could not execute command: " + join(commands, " "));
                failed();
                return;
            }

            StringLogger procOutput = new StringLogger(getProjectName() + String.valueOf(getProjectJobID()), p.getInputStream());
            procOutput.start();

            if (p.waitFor() == 0) {
                finished();
            } else {
                failed();
            }

            procOutput.join();
        } catch (InterruptedException | IOException ex) {
            /*
             * job was aborted
             */
            try {
                failed();
                setState(JobState.ABORTED);
                setFinishDate();
            } catch (JobException ex1) {
                Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    @Override
    public void failed() {
        Logger.getLogger(MGXCWLJob.class.getName()).log(Level.INFO, "Job failed, removing partial results");

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FAILED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void delete() {
        try (Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

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
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
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
            try (PreparedStatement stmt = conn.prepareStatement("SELECT job_state FROM job WHERE id=? FOR UPDATE")) {
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
        } catch (SQLException | MGXDispatcherException ex) {
            throw new JobException(ex);
        }

        JobState dbState = getState();
        if (dbState != state) {
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.INFO, "DB inconsistent, expected {0}, got {1}", new Object[]{state, dbState});
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
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex);
        }
        return JobState.values()[state];
    }

    @Override
    public void finished() {

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

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FINISHED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGXCWLJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String getProjectClass() {
        return "MGX-2";
    }

    @Override
    public boolean validate() throws JobException {
        return true;
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }

    private static String join(String[] elems, String separator) {
        if (elems == null || elems.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(elems[0]);
        for (int i = 1; i < elems.length; i++) {
            sb.append(separator);
            sb.append(elems[i]);
        }
        return sb.toString();
    }
}
