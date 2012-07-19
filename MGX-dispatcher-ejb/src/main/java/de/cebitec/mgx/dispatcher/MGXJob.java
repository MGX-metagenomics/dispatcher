package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import de.cebitec.mgx.dto.dto.JobDTO.JobState;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author sjaenick
 */
public class MGXJob implements Runnable {

    //
    private int queueId;
    private long mgxJobId;
    private String projName;
    private String conveyorGraph;
    private int priority;
    private Dispatcher dispatcher;
    private DispatcherConfiguration config;
    private Connection pconn = null;

    public MGXJob(Dispatcher d, DispatcherConfiguration cfg, String projName, long mgxJobId) throws MGXDispatcherException {
        dispatcher = d;
        config = cfg;
        this.projName = projName;
        this.mgxJobId = mgxJobId;
        priority = 500;
        pconn = getProjectConnection(projName);
        conveyorGraph = lookupGraphFile(projName, mgxJobId);
    }

    @Override
    public void run() {
        JobState state = null;
        try {
            state = getState();
        } catch (MGXDispatcherException ex) {
            dispatcher.log(ex.getMessage());
            return;
        }

        if (state == JobState.IN_DELETION) {
            this.delete();
        } else {
            this.execute();
        }

        dispatcher.handleExitingJob(this);
    }

    private void execute() {
        // build up command string
        List<String> commands = new ArrayList<>();
        commands.add(config.getConveyorExecutable());
        commands.add(conveyorGraph);
        commands.add(projName);
        commands.add(String.valueOf(mgxJobId));

        StringBuilder cmd = new StringBuilder();
        for (String s : commands) {
            cmd.append(s);
            cmd.append(" ");
        }

        dispatcher.log("EXECUTING COMMAND: " + cmd.toString());

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
                dispatcher.log(ex.getMessage());
                pconn = getProjectConnection(projName);
                setState(JobState.ABORTED);
                setFinishDate();
                return;
            } finally {
                p.destroy();
            }

            // reconnect to database
            pconn = getProjectConnection(projName);
            if (exitCode == 0) {
                setFinished();
            } else {
                setState(JobState.FAILED);
            }
            setFinishDate();

        } catch (MGXDispatcherException | SQLException ex) {
            dispatcher.log(ex.getMessage());
        }


    }

    private void delete() {
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
                dispatcher.log(ex1.getMessage());
            }
            dispatcher.log(ex.getMessage());
        } finally {
            close(stmt, null);
        }
    }

    public String getConveyorGraph() {
        return conveyorGraph;
    }

    public long getMgxJobId() {
        return mgxJobId;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int prio) {
        priority = prio;
    }

    public String getProjectName() {
        return projName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer qId) {
        queueId = qId;
    }

    public void setStartDate() throws MGXDispatcherException {
        PreparedStatement stmt = null;
        try {
            stmt = pconn.prepareStatement("UPDATE job SET startdate=NOW() WHERE id=?");
            stmt.setLong(1, mgxJobId);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new MGXDispatcherException(ex.getMessage());
        } finally {
            close(stmt, null);
        }
    }

    public void setFinishDate() throws MGXDispatcherException {
        PreparedStatement stmt = null;
        try {
            stmt = pconn.prepareStatement("UPDATE job SET finishdate=NOW() WHERE id=?");
            stmt.setLong(1, mgxJobId);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new MGXDispatcherException(ex.getMessage());
        } finally {
            close(stmt, null);
        }
    }

    public void setState(JobState state) throws MGXDispatcherException {
        PreparedStatement stmt = null;
        String sql = "UPDATE job SET job_state=? WHERE id=?";
        try {
            stmt = pconn.prepareStatement(sql);
            stmt.setLong(1, state.ordinal());
            stmt.setLong(2, mgxJobId);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new MGXDispatcherException(ex.getMessage());
        } finally {
            close(stmt, null);
        }
    }

    public JobState getState() throws MGXDispatcherException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = "SELECT job_state FROM job WHERE id=?";
        int state = -1;

        try {
            stmt = pconn.prepareStatement(sql);
            stmt.setLong(1, mgxJobId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                state = rs.getInt(1);
            }
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        } finally {
            close(stmt, rs);
        }
        return JobState.values()[state];
    }

    private String lookupGraphFile(String projName, long jobId) throws MGXDispatcherException {
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
            dispatcher.log(ex.getMessage());
        } finally {
            close(stmt, rs);
        }

        return file;
    }

    private void setFinished() {

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
        } catch (Exception ex) {
            try {
                pconn.rollback();
            } catch (SQLException ex1) {
                dispatcher.log(ex1.getMessage());
            }
            dispatcher.log(ex.getMessage());
        } finally {
            close(stmt, null);
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
            dispatcher.log(ex.getMessage());
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
            dispatcher.log(ex.getMessage());
        }
    }
}
