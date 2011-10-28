package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import de.cebitec.mgx.dto.JobDTO.JobState;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author sjaenick
 */
public class MGXJob implements Runnable {

    //
    private Integer queueId;
    private Long mgxJobId;
    private String projName;
    private String conveyorGraph;
    private int priority;
    private Dispatcher dispatcher;

    public MGXJob(Dispatcher d, String projName, Long mgxJobId) throws MGXDispatcherException {
        dispatcher = d;
        this.projName = projName;
        this.mgxJobId = mgxJobId;
        conveyorGraph = lookupGraphFile(projName, mgxJobId);
        priority = 500;
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
        List<String> commands = new LinkedList<String>();
        commands.add(dispatcher.getConfig().getConveyorExecutable());
        commands.add(conveyorGraph);
        commands.add(projName);
        commands.add(mgxJobId.toString());

        StringBuilder cmd = new StringBuilder();
        for (String s : commands) {
            cmd.append(s);
            cmd.append(" ");
        }

        dispatcher.log("EXECUTING COMMAND: " + cmd.toString());

        try {
            setState(JobState.RUNNING);
            setStartDate();

            try {
                Process p = Runtime.getRuntime().exec(commands.toArray(new String[0]));
                try {
                    int exitCode = p.waitFor();
                    if (exitCode == 0) {
                        setState(JobState.FINISHED);
                    } else {
                        setState(JobState.FAILED);
                    }
                    setFinishDate();
                } catch (InterruptedException ex) {
                    setState(JobState.ABORTED);
                } finally {
                    p.destroy();
                }
            } catch (IOException ex) {
                dispatcher.log(ex.getMessage());
                setFinishDate();
                setState(JobState.FAILED);
            }

        } catch (MGXDispatcherException ex) {
            dispatcher.log(ex.getMessage());
        }


    }

    private void delete() {
        try {

            // remove observations
            Connection pconn = getProjectConnection(projName);
            String sql = String.format("DELETE FROM observation WHERE jobid=%s", mgxJobId.toString());
            Statement s = pconn.createStatement();
            s.executeUpdate(sql);
            s.close();

            /*
             * we can't delete orphan attributes, since there might be other analysis jobs
             * running that rely on them; there's a short period of time between attribute
             * creation and referencing the attribute in the observation table
             */

            // delete the job
            sql = String.format("DELETE FROM job WHERE id=%s", mgxJobId.toString());
            s = pconn.createStatement();
            s.executeUpdate(sql);
            s.close();

            pconn.close();
        } catch (Exception ex) {
            dispatcher.log(ex.getMessage());
        }
    }

    public String getConveyorGraph() {
        return conveyorGraph;
    }

    public Long getMgxJobId() {
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

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer qId) {
        queueId = qId;
    }

    public void setStartDate() throws MGXDispatcherException {
        Connection pconn = getProjectConnection(projName);
        String sql = String.format("UPDATE job SET startdate=NOW() WHERE id=%s", mgxJobId.toString());
        try {
            Statement s = pconn.createStatement();
            s.executeUpdate(sql);
            s.close();
            pconn.close();
        } catch (SQLException ex) {
        }
    }

    public void setFinishDate() throws MGXDispatcherException {
        Connection pconn = getProjectConnection(projName);
        String sql = String.format("UPDATE job SET finishdate=NOW() WHERE id=%s", mgxJobId.toString());
        try {
            Statement s = pconn.createStatement();
            s.executeUpdate(sql);
            s.close();
            pconn.close();
        } catch (SQLException ex) {
        }
    }

    public void setState(JobState state) throws MGXDispatcherException {
        Connection pconn = getProjectConnection(projName);
        String sql = String.format("UPDATE job SET job_state=%d WHERE id=%s", state.ordinal(), mgxJobId.toString());
        try {
            Statement s = pconn.createStatement();
            s.executeUpdate(sql);
            s.close();
            pconn.close();
        } catch (SQLException ex) {
            throw new MGXDispatcherException(ex.getMessage());
        }
    }

    public JobState getState() throws MGXDispatcherException {
        Connection pconn = getProjectConnection(projName);
        String sql = String.format("SELECT job_state FROM job WHERE id=%s", mgxJobId.toString());
        Integer state = null;
        try {
            Statement s = pconn.createStatement();
            ResultSet rs = s.executeQuery(sql);
            if (rs.next()) {
                state = rs.getInt(1);
            }
            rs.close();
            s.close();
            pconn.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        }
        return JobState.values()[state];
    }

    private String lookupGraphFile(String projName, Long jobId) throws MGXDispatcherException {
        String file = null;
        Connection pconn = getProjectConnection(projName);

        String sql = String.format("SELECT Tool.xml_file FROM Job LEFT JOIN Tool ON (Job.tool_id=Tool.id) WHERE Job.id=%s;", jobId.toString());

        try {
            Statement stmt = pconn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                file = rs.getString(1);
            }
            rs.close();
            stmt.close();
            pconn.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        }

        return file;
    }

    private Connection getProjectConnection(String projName) throws MGXDispatcherException {
        GPMSHelper gpms = new GPMSHelper(dispatcher);
        String url = gpms.getJDBCURLforProject(projName);

        DispatcherConfiguration cfg = dispatcher.getConfig();

        Connection c = null;
        try {
            Class.forName(cfg.getMGXDriverClass());
            c = DriverManager.getConnection(url, cfg.getMGXUser(), cfg.getMGXPassword());
        } catch (Exception e) {
        }
        return c;
    }
}
