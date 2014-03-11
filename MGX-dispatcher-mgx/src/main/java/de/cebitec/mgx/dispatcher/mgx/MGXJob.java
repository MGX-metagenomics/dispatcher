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
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
public class MGXJob extends JobI {

    // project-specific job id
    private final long mgxJobId;
    private final String projectName;
    private final String conveyorGraph;
    private final String persistentDir;
    private final String conveyorValidate;
    private final String conveyorExecutable;
    private final ConnectionProviderI cc;
    //private final DispatcherConfiguration config;
    private Connection pconn;
    private final static Logger logger = Logger.getLogger(MGXJob.class.getPackage().getName());

    public MGXJob(Dispatcher disp, String conveyorExec, String conveyorValidate, String persistentDir,
            ConnectionProviderI cc, String projName,
            long mgxJobId) throws MGXDispatcherException {

        super(disp, JobI.DEFAULT_PRIORITY);
        //config = dispCfg;
        this.projectName = projName;
        this.mgxJobId = mgxJobId;
        this.conveyorValidate = conveyorValidate;
        this.conveyorExecutable = conveyorExec;
        this.persistentDir = persistentDir;
        this.cc = cc;
        pconn = getProjectConnection(projectName);
        conveyorGraph = lookupGraphFile(mgxJobId);
    }

    @Override
    public void prepare() {
        //
    }

    @Override
    public void process() {
        // build up command string
        List<String> commands = new ArrayList<>();
        commands.add(conveyorExecutable);
        commands.add(conveyorGraph);
        commands.add(projectName);
        commands.add(String.valueOf(mgxJobId));

        StringBuilder cmd = new StringBuilder();
        for (String s : commands) {
            cmd.append(s);
            cmd.append(" ");
        }
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

        Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", cmd.toString().trim());

        try {
            // disconnect from database
            pconn.close();
            pconn = null;

            Process p = null;
            int exitCode = -1;
            try {
                p = Runtime.getRuntime().exec(commands.toArray(new String[]{}));
                exitCode = p.waitFor();
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
                pconn = getProjectConnection(projectName);
                setState(JobState.ABORTED);
                setFinishDate();
                return;
            } finally {
                if (p != null) {
                    p.destroy();
                }
            }

            // reconnect to database
            pconn = getProjectConnection(projectName);
            if (exitCode == 0) {
                finished();
            } else {
                setState(JobState.FAILED);
            }
            setFinishDate();

        } catch (MGXDispatcherException | JobException | SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void failed() {
        PreparedStatement stmt = null;
        try {
            pconn.setAutoCommit(false);

            // remove observations
            stmt = pconn.prepareStatement("DELETE FROM observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            // remove observations
            stmt = pconn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            // remove attributes
            stmt = pconn.prepareStatement("DELETE FROM attribute WHERE job_id=?");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            /*
             * we can't delete orphan attributetypes, since there might be other
             * analysis jobs running that rely on them; there's a short period
             * of time between attributetype creation and referencing the
             * attributetype in the attribute table
             */
            // mark job failed
            stmt = pconn.prepareStatement("UPDATE job SET job_state=? WHERE id=?");
            stmt.setLong(1, JobState.FAILED.ordinal());
            stmt.setLong(2, mgxJobId);
            stmt.execute();

            pconn.commit();
            pconn.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            try {
                pconn.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            }
        } finally {
            close(stmt, null);
        }
    }

    @Override
    public void delete() {
        PreparedStatement stmt = null;
        try {
            pconn.setAutoCommit(false);

            // remove observations
            stmt = pconn.prepareStatement("DELETE FROM observation WHERE attributeid IN (SELECT id FROM attribute WHERE job_id=?)");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            // remove observations
            stmt = pconn.prepareStatement("DELETE FROM attributecount WHERE attr_id IN (SELECT id FROM attribute WHERE job_id=?)");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            // remove attributes
            stmt = pconn.prepareStatement("DELETE FROM attribute WHERE job_id=?");
            stmt.setLong(1, mgxJobId);
            stmt.execute();
            stmt.close();

            /*
             * we can't delete orphan attributetypes, since there might be other
             * analysis jobs running that rely on them; there's a short period
             * of time between attributetype creation and referencing the
             * attributetype in the attribute table
             */
            // delete the job
            stmt = pconn.prepareStatement("DELETE FROM job WHERE id=?");
            stmt.setLong(1, mgxJobId);
            stmt.execute();

            pconn.commit();
            pconn.setAutoCommit(true);
        } catch (SQLException ex) {
            try {
                pconn.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(stmt, null);
        }
    }

    @Override
    public String getConveyorGraph() {
        return conveyorGraph;
    }

    @Override
    public long getProjectJobID() {
        return mgxJobId;
    }

    @Override
    public String getProjectName() {
        return projectName;
    }

    private void setStartDate() throws JobException {
        int numRows = 0;
        try (PreparedStatement stmt = pconn.prepareStatement("UPDATE job SET startdate=NOW() WHERE id=?")) {
            stmt.setLong(1, mgxJobId);
            numRows = stmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }
        if (numRows != 1) {
            throw new JobException("Could not set start date");
        }
    }

    private void setFinishDate() throws JobException {
        int numRows = 0;
        try (PreparedStatement stmt = pconn.prepareStatement("UPDATE job SET finishdate=NOW() WHERE id=?")) {
            stmt.setLong(1, mgxJobId);
            numRows = stmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }
        if (numRows != 1) {
            throw new JobException("Could not set finish date");
        }
    }

    @Override
    public void setState(JobState state) throws JobException {
        Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "{0}: state change {1} to {2}", new Object[]{mgxJobId, getState(), state});
        String sql = "UPDATE job SET job_state=? WHERE id=? RETURNING job_state";
        try (Connection conn = getProjectConnection(projectName)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);

            // acquire row lock
            try (PreparedStatement stmt = conn.prepareStatement("SELECT job_state FROM job where id=? FOR UPDATE")) {
                stmt.setLong(1, mgxJobId);
                stmt.execute();
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, state.ordinal());
                stmt.setLong(2, mgxJobId);
                try (ResultSet rs = stmt.executeQuery()) {
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
            conn.close();
        } catch (Exception ex) {
            throw new JobException(ex);
        }

        try {
            // reconnect to database
            pconn = getProjectConnection(projectName);
            pconn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        } catch (SQLException | MGXDispatcherException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException("reconnect failed: " + ex.getMessage());
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

        try (PreparedStatement stmt = pconn.prepareStatement(sql)) {
            stmt.setLong(1, mgxJobId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    state = rs.getInt(1);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex);
        }
        return JobState.values()[state];
    }

    private String lookupGraphFile(long jobId) throws MGXDispatcherException {
        String file = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = "SELECT Tool.xml_file FROM Job LEFT JOIN Tool ON (Job.tool_id=Tool.id) WHERE Job.id=?";

        try {
            stmt = pconn.prepareStatement(sql);
            stmt.setLong(1, jobId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                file = rs.getString(1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(stmt, rs);
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

        PreparedStatement stmt = null;
        try {
            // set the job to finished state
            pconn.setAutoCommit(false);
            // create assignment counts for attributes belonging to this job
            String sql = "INSERT INTO attributecount "
                    + "SELECT attribute.id, count(attribute.id) FROM attribute "
                    + "LEFT JOIN observation ON (attribute.id = observation.attr_id) "
                    + "WHERE job_id=? GROUP BY attribute.id ORDER BY attribute.id";
            stmt = pconn.prepareStatement(sql);
            stmt.setLong(1, mgxJobId);
            stmt.execute();

            pconn.commit();
            pconn.setAutoCommit(true);
        } catch (SQLException ex) {
            try {
                pconn.rollback();
            } catch (SQLException ex1) {
                Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(stmt, null);
        }

        /*
         * remove stdout/stderr files for finished jobs; keep
         * them for debugging purposes, otherwise
         */
        StringBuilder sb = new StringBuilder()
                .append(persistentDir)
                .append(File.separator)
                .append(projectName)
                .append(File.separator)
                .append("jobs")
                .append(File.separator)
                .append(String.valueOf(mgxJobId))
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

    private Connection getProjectConnection(String projName) throws MGXDispatcherException {
        return cc.getProjectConnection(projName);
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
        // build up command string
        List<String> commands = new ArrayList<>();
        commands.add(conveyorValidate);
        commands.add(getConveyorGraph());
        commands.add(getProjectName());
        commands.add(String.valueOf(getProjectJobID()));

        String[] argv = commands.toArray(new String[]{});

        StringBuilder output = new StringBuilder();
        Integer exitCode = null;
        try {
            Process p = Runtime.getRuntime().exec(argv);
            try (BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String s;
                while ((s = stdout.readLine()) != null) {
                    output.append(s);
                }
            }

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

        throw new JobException(output.toString());
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }
}
