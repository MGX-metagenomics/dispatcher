package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.gpms.util.GPMSDataLoaderI;
import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.JobException;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import de.cebitec.mgx.sequence.DNASequenceI;
import de.cebitec.mgx.sequence.SeqReaderFactory;
import de.cebitec.mgx.sequence.SeqReaderI;
import de.cebitec.mgx.sequence.SeqStoreException;
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
public class MGX1ConveyorJob extends JobI {

    // project-specific job id
    private final String conveyorGraph;
    private final String persistentDir;
    private final String conveyorValidate;
    private final String conveyorExecutable;
    private final ConnectionProviderI cc;
    private final GPMSDataLoaderI loader;
    private final static Logger logger = Logger.getLogger(MGX1ConveyorJob.class.getPackage().getName());

    public MGX1ConveyorJob(Dispatcher disp,
            String conveyorExec, String conveyorValidate,
            String persistentDir,
            ConnectionProviderI cc, GPMSDataLoaderI loader, String projName,
            long mgxJobId) throws MGXDispatcherException {

        super(disp, mgxJobId, projName, JobI.DEFAULT_PRIORITY);
        this.conveyorValidate = conveyorValidate;
        this.conveyorExecutable = conveyorExec;
        this.persistentDir = persistentDir;
        this.cc = cc;
        this.loader = loader;
        conveyorGraph = lookupGraphFile(mgxJobId);
    }

    @Override
    public void prepare() {
        //
    }

    @Override
    public void process() {

        String[] commands = new String[4];
        commands[0] = conveyorExecutable;
        commands[1] = conveyorGraph;
        commands[2] = getProjectName();
        commands[3] = String.valueOf(getProjectJobID());

        try {
            setState(JobState.RUNNING);
            setStartDate();
        } catch (JobException ex) {
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            failed();
            return;
        }

        Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", join(commands, " "));

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
                Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    @Override
    public void failed() {
        Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.INFO, "Job {0} in project {1} failed, removing partial results",
                new Object[]{getProjectJobID(), getProjectName()});

        try (Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

            // remove observations
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM observation WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
                stmt.close();
            }

            // remove attributecounts
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
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FAILED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.INFO, "DB inconsistent, expected {0}, got {1}", new Object[]{state, dbState});
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }

        return file;
    }

    @Override
    public void finished() {
        Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.INFO, "Job {0} in project {1} finished successfully.",
                new Object[]{getProjectJobID(), getProjectName()});

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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            try {
                setState(JobState.FAILED);
            } catch (JobException ex1) {
                Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex1);
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

        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FINISHED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private Connection getProjectConnection() throws MGXDispatcherException {
        return cc.getProjectConnection(loader, getProjectName());
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
            Logger.getLogger(MGX1ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String getProjectClass() {
        return "MGX";
    }

    private String getDBFile() throws JobException {
        String sql = "SELECT seqrun.dbfile FROM job LEFT JOIN seqrun ON (job.seqrun_id=seqrun.id) WHERE job.id=?";
        String dbFile = null;
        try (Connection conn = getProjectConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, getProjectJobID());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        dbFile = rs.getString(1);
                    }
                }
            }
        } catch (SQLException | MGXDispatcherException ex) {
            throw new JobException(ex.getMessage());
        }
        return dbFile;
    }

    @Override
    public boolean validate() throws JobException {

        // make sure sequence store can be accessed
        try (SeqReaderI<? extends DNASequenceI> reader = SeqReaderFactory.getReader(getDBFile())) {
            if (reader == null || !reader.hasMoreElements()) {
                throw new JobException("Unable to access sequence store");
            }
        } catch (SeqStoreException ex) {
            throw new JobException(ex.getMessage());
        }

        File validate = new File(conveyorValidate);
        if (!validate.canRead() && validate.canExecute()) {
            throw new JobException("Unable to access Conveyor executable.");
        }

        File graph = new File(conveyorGraph);
        if (!graph.canRead()) {
            throw new JobException("Cannot read workflow file");
        }

        // build up command string
        String[] commands = new String[4];
        commands[0] = conveyorValidate;
        commands[1] = conveyorGraph;
        commands[2] = getProjectName();
        commands[3] = String.valueOf(getProjectJobID());

        ProcessBuilder pBuilder = new ProcessBuilder(commands);
        pBuilder.redirectErrorStream(true);

        Process p = null;

        try {
            p = pBuilder.start();
            if (p == null) {
                log("Could not execute command: " + join(commands, " "));
                setState(JobState.FAILED);
                return false;
            }

            StringLogger procOutput = new StringLogger(getProjectName() + String.valueOf(getProjectJobID()), p.getInputStream());
            procOutput.start();

            if (p.waitFor() == 0) {
                procOutput.join();
                return true;
            } else {
                log("Validation failed, commmand was " + join(commands, " "));
                procOutput.join();
                String output = procOutput.getOutput();
                throw new JobException(output != null ? output : "No output available.");
            }
        } catch (IOException | InterruptedException ex) {
            log(ex.getMessage());
            failed();
            return false;
        } finally {
            if (p != null) {
                try {
                    p.getInputStream().close();
                } catch (IOException ex) {
                    Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
                }
                p.destroy();
            }
        }
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
