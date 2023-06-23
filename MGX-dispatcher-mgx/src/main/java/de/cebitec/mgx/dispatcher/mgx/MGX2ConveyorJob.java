package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.gpms.util.GPMSDataLoaderI;
import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.common.ToolScope;
import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.JobException;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import de.cebitec.mgx.dispatcher.mgx.util.StringUtil;
import de.cebitec.mgx.seqcompression.SequenceException;
import de.cebitec.mgx.sequence.DNASequenceI;
import de.cebitec.mgx.sequence.SeqReaderFactory;
import de.cebitec.mgx.sequence.SeqReaderI;
import de.cebitec.mgx.streamlogger.StringLogger;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
public class MGX2ConveyorJob extends JobI {

    // project-specific job id
    private final File conveyorGraph;
    private final String persistentDir;
    private final String conveyorValidate;
    private final String conveyorExecutable;
    private final ConnectionProviderI cc;
    private final GPMSDataLoaderI loader;
    private final static Logger logger = Logger.getLogger(MGX2ConveyorJob.class.getPackage().getName());

    public MGX2ConveyorJob(DispatcherI disp,
            String conveyorExec, String conveyorValidate,
            File workflowDefinition,
            String persistentDir,
            ConnectionProviderI cc, GPMSDataLoaderI loader, String projName,
            long mgxJobId) throws MGXDispatcherException {

        super(disp, mgxJobId, projName, JobI.DEFAULT_PRIORITY);
        this.conveyorValidate = conveyorValidate;
        this.conveyorExecutable = conveyorExec;
        this.persistentDir = persistentDir;
        this.cc = cc;
        this.loader = loader;
        conveyorGraph = workflowDefinition;
    }

    @Override
    public void prepare() {
        //
    }

    @Override
    public void process() {

        String[] commands = new String[4];
        commands[0] = conveyorExecutable;
        commands[1] = conveyorGraph.getAbsolutePath();
        commands[2] = getProjectName();
        commands[3] = String.valueOf(getProjectJobID());

        try {
            setState(JobState.RUNNING);
            setStartDate();
        } catch (JobException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            failed();
            return;
        }

        Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", StringUtil.join(commands, " "));

        Process p = null;

        try {
            ProcessBuilder pBuilder = new ProcessBuilder(commands);
            pBuilder.redirectErrorStream(true);

            p = pBuilder.start();
            if (p == null) {
                log("Could not execute command: " + StringUtil.join(commands, " "));
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
                Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    @Override
    public void failed() {
        Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.INFO, "Job {0} in project {1} failed, removing partial results",
                new Object[]{getProjectJobID(), getProjectName()});

        try ( Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

            // remove observations
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM observation WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM gene_observation WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            // remove attributecounts
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            // remove attributes
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM attribute WHERE job_id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
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
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }

        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FAILED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void delete() {
        try ( Connection conn = getProjectConnection()) {
            conn.setAutoCommit(false);

            // remove observations
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM gene_observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            // remove attribute counts
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            // remove attributes
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM attribute WHERE job_id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            /*
             * we can't delete orphan attributetypes, since there might be other
             * analysis jobs running that rely on them; there's a short period
             * of time between attributetype creation and referencing the
             * attributetype in the attribute table
             */
            // delete the job
            try ( PreparedStatement stmt = conn.prepareStatement("DELETE FROM job WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void setStartDate() throws JobException {
        int numRows = 0;
        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement("UPDATE job SET startdate=NOW() WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                numRows = stmt.executeUpdate();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }

        if (numRows != 1) {
            throw new JobException("Could not set start date");
        }
    }

    private void setFinishDate() throws JobException {
        int numRows = 0;
        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement("UPDATE job SET finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, getProjectJobID());
                numRows = stmt.executeUpdate();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
        try ( Connection conn = getProjectConnection()) {
            //conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            // acquire row lock
            try ( PreparedStatement stmt = conn.prepareStatement("SELECT job_state FROM job WHERE id=? FOR UPDATE")) {
                stmt.setLong(1, getProjectJobID());
                stmt.execute();
            }

            try ( PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, state.ordinal());
                stmt.setLong(2, getProjectJobID());
                try ( ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        JobState newState = JobState.values()[rs.getInt(1)];
                        if (newState != state) {
                            throw new JobException("DB update failed, expected " + state + ", got " + newState);
                        }
                    }
                }
            }
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException | MGXDispatcherException ex) {
            throw new JobException(ex);
        }

        JobState dbState = getState();
        if (dbState != state) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.INFO, "DB inconsistent, expected {0}, got {1}", new Object[]{state, dbState});
            throw new JobException("DB inconsistent, expected " + state + ", got " + dbState);
        }
    }

    @Override
    public JobState getState() throws JobException {
        String sql = "SELECT job_state FROM job WHERE id=?";
        int state = -1;

        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, getProjectJobID());
                try ( ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        state = rs.getInt(1);
                    }
                }
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex);
        }
        return JobState.values()[state];
    }

    private ToolScope getToolScope(long jobId) throws MGXDispatcherException {
        ToolScope scope = null;
        String sql = "SELECT Tool.scope FROM Job LEFT JOIN Tool ON (Job.tool_id=Tool.id) WHERE Job.id=?";

        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, jobId);
                try ( ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        scope = ToolScope.values()[rs.getInt(1)];
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new MGXDispatcherException(ex.getMessage());
        }

        return scope;
    }

    @Override
    public void finished() {
        Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.INFO, "Job {0} in project {1} finished successfully.",
                new Object[]{getProjectJobID(), getProjectName()});

        ToolScope scope;
        try {
            scope = getToolScope(getProjectJobID());
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        try ( Connection conn = getProjectConnection()) {
            // set the job to finished state
            conn.setAutoCommit(false);

            switch (scope) {
                case READ:
                    // create assignment counts for attributes belonging to this job
                    String sql = "INSERT INTO attributecount "
                            + "SELECT attribute.id, read.seqrun_id, count(attribute.id) FROM attribute "
                            + "LEFT JOIN observation ON (attribute.id = observation.attr_id) "
                            + "LEFT JOIN read ON (observation.seq_id=read.id) "
                            + "WHERE job_id=? GROUP BY attribute.id, read.seqrun_id ORDER BY attribute.id";
                    try ( PreparedStatement stmt = conn.prepareStatement(sql)) {
                        stmt.setLong(1, getProjectJobID());
                        stmt.execute();
                    }
                    break;
                case ASSEMBLY:
                    // noop
                    break;
                case GENE_ANNOTATION:
                    // create assignment counts for attributes belonging to this job
                    String sql2 = "INSERT INTO attributecount "
                            + "SELECT attribute.id, gene_coverage.run_id, sum(gene_coverage.coverage) FROM attribute "
                            + "LEFT JOIN gene_observation ON (attribute.id = gene_observation.attr_id) "
                            + "LEFT JOIN gene ON (gene_observation.gene_id=gene.id) "
                            + "LEFT JOIN gene_coverage ON (gene.id=gene_coverage.gene_id) "
                            + "WHERE job_id=? AND gene_coverage.coverage > 0 "
                            + "GROUP BY attribute.id, gene_coverage.run_id ORDER BY attribute.id";
                    try ( PreparedStatement stmt = conn.prepareStatement(sql2)) {
                        stmt.setLong(1, getProjectJobID());
                        stmt.execute();
                    }
                    break;
                default:
                    // noop
                    Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, "Unrecognized tool scope {0}.", scope);
                    break;
            }

            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            try {
                setState(JobState.FAILED);
            } catch (JobException ex1) {
                Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex1);
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

        // set job to finished state and remove api key
        try ( Connection conn = getProjectConnection()) {
            try ( PreparedStatement stmt = conn.prepareStatement("UPDATE job SET job_state=?, apikey=NULL, finishdate=NOW() WHERE id=?")) {
                stmt.setLong(1, JobState.FINISHED.ordinal());
                stmt.setLong(2, getProjectJobID());
                stmt.execute();
            }
        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String getProjectClass() {
        return "MGX-2";
    }

    private List<String> getDBFiles() throws JobException {

        ToolScope scope = null;
        try {
            scope = getToolScope(getProjectJobID());
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }

        List<String> ret = new ArrayList<>();

        switch (scope) {
            case READ:
            case ASSEMBLY:

                String sql = "SELECT seqruns FROM job WHERE job.id=?";
                Long[] values = null;
                try ( Connection conn = getProjectConnection()) {
                    try ( PreparedStatement stmt = conn.prepareStatement(sql)) {
                        stmt.setLong(1, getProjectJobID());
                        try ( ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                values = (Long[]) rs.getArray(1).getArray();
                            }
                        }
                    }
                } catch (SQLException | MGXDispatcherException ex) {
                    throw new JobException(ex.getMessage());
                }

                if (values == null || values.length != 1) {
                    throw new JobException("Cannot process multiple seqruns.");
                }

                for (Long runId : values) {

                    StringBuilder sb = new StringBuilder()
                            .append(persistentDir)
                            .append(File.separator)
                            .append(getProjectName())
                            .append(File.separator)
                            .append("seqruns")
                            .append(File.separator)
                            .append(String.valueOf(runId));
                    ret.add(sb.toString());
                }
        }

        return ret;
    }

    @Override
    public boolean validate() throws JobException {

        ToolScope scope = null;
        try {
            scope = getToolScope(getProjectJobID());
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGX2ConveyorJob.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (scope == ToolScope.READ || scope == ToolScope.ASSEMBLY) {
            // make sure sequence store can be accessed
            for (String runFile : getDBFiles()) {
                try ( SeqReaderI<? extends DNASequenceI> reader = SeqReaderFactory.getReader(runFile)) {
                    if (reader == null || !reader.hasMoreElements()) {
                        throw new JobException("Unable to access sequence store");
                    }
                } catch (SequenceException ex) {
                    throw new JobException(ex.getMessage());
                }
            }
        }

        File validate = new File(conveyorValidate);
        if (!validate.canRead() && validate.canExecute()) {
            throw new JobException("Unable to access Conveyor executable " + conveyorValidate);
        }

        // build up command string
        String[] commands = new String[4];
        commands[0] = conveyorValidate;
        commands[1] = conveyorGraph.getAbsolutePath();
        commands[2] = getProjectName();
        commands[3] = String.valueOf(getProjectJobID());

        ProcessBuilder pBuilder = new ProcessBuilder(commands);
        pBuilder.redirectErrorStream(true);

        Process p = null;

        try {
            p = pBuilder.start();
            if (p == null) {
                log("Could not execute command: " + StringUtil.join(commands, " "));
                setState(JobState.FAILED);
                return false;
            }

            StringLogger procOutput = new StringLogger(getProjectName() + String.valueOf(getProjectJobID()), p.getInputStream());
            procOutput.start();

            if (p.waitFor() == 0) {
                procOutput.join();
                return true;
            } else {
                log("Validation failed, commmand was " + StringUtil.join(commands, " "));
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
}
