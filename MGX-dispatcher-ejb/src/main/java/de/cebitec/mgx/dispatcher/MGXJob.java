package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.common.JobState;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
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
public class MGXJob extends JobI {

    // project-specific job id
    private final long mgxJobId;
    private final String projectName;
    private final String conveyorGraph;
    private final Dispatcher dispatcher;
    private final DispatcherConfiguration config;
    private Connection pconn;

    public MGXJob(Dispatcher disp, DispatcherConfiguration dispCfg, String projName, long mgxJobId) throws MGXDispatcherException {
        super(disp, JobI.DEFAULT_PRIORITY);
        dispatcher = disp;
        config = dispCfg;
        this.projectName = projName;
        this.mgxJobId = mgxJobId;
        pconn = getProjectConnection(projName);
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
        commands.add(config.getConveyorExecutable());
        commands.add(conveyorGraph);
        commands.add(projectName);
        commands.add(String.valueOf(mgxJobId));

        StringBuilder cmd = new StringBuilder();
        for (String s : commands) {
            cmd.append(s);
            cmd.append(" ");
        }

        Logger.getLogger(MGXJob.class.getName()).log(Level.INFO, "EXECUTING COMMAND: {0}", cmd.toString().trim());

        try {
            setState(JobState.RUNNING);
            setStartDate();

            // disconnect from database
            pconn.close();
            pconn = null;

            Process p = null;
            int exitCode = -1;
            try {
                p = Runtime.getRuntime().exec(commands.toArray(new String[0]));
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
        } catch (Exception ex) {
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
        String sql = "UPDATE job SET job_state=? WHERE id=?";
        int numRows = 0;
        try (PreparedStatement stmt = pconn.prepareStatement(sql)) {
            stmt.setLong(1, state.ordinal());
            stmt.setLong(2, mgxJobId);
            numRows = stmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
            throw new JobException(ex.getMessage());
        }
        if (numRows != 1) {
            throw new JobException("Could not set job state");
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

        PreparedStatement stmt = null;
        try {
            // set the job to finished state
            pconn.setAutoCommit(false);
            stmt = pconn.prepareStatement("UPDATE job SET job_state=? WHERE id=?");
            stmt.setLong(1, JobState.FINISHED.ordinal());
            stmt.setLong(2, mgxJobId);
            stmt.execute();

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
                .append(config.getMGXPersistentDir())
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
        GPMSHelper gpms = new GPMSHelper(dispatcher, config);
        String url = gpms.getJDBCURLforProject(projName);

        Connection c = null;
        try {
            Class.forName(config.getMGXDriverClass());
            c = DriverManager.getConnection(url, config.getMGXUser(), config.getMGXPassword());
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(MGXJob.class.getName()).log(Level.SEVERE, null, ex);
        }
        assert c != null;
        return c;
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
}
