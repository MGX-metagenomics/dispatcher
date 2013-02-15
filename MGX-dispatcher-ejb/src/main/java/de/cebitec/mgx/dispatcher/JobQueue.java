package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.*;
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
@Singleton
@Startup
public class JobQueue {

    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/DispatcherConfiguration")
    protected DispatcherConfiguration config;
    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/Dispatcher")
    protected Dispatcher dispatcher;
    protected Connection jobqueue = null;

    @PostConstruct
    public void init() {
        try {
            Class.forName(config.getJobQueueDriverClass());
            jobqueue = DriverManager.getConnection("jdbc:sqlite:" + config.getJobQueueFilename());
            createJobQueue();
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(JobQueue.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void close() throws SQLException {
        jobqueue.close();
    }

    public int createJob(JobI job) throws MGXDispatcherException {

        /*
         * create new job entry in the dispatcher queue
         */
        int queueId = -1;
        try {
            String sql = "INSERT INTO jobqueue (project, mgxjob_id, priority) VALUES (?, ?, ?)";
            //sql = String.format(sql, job.getProjectName(), job.getMgxJobId(), job.getPriority());
            PreparedStatement stmt = jobqueue.prepareStatement(sql);
            stmt.setString(1, job.getProjectName());
            stmt.setLong(2, job.getProjectJobID());
            stmt.setInt(3, job.getPriority());
            stmt.executeUpdate();

            stmt = jobqueue.prepareStatement("SELECT last_insert_rowid()");
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                queueId = rs.getInt(1);
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }
        if (queueId != -1) {
            job.setQueueID(queueId);
            return queueId;
        }
        throw new MGXDispatcherException("No queue ID returned.");
    }

    public void removeJob(JobI job) {
        try {
            String sql = "DELETE FROM jobqueue WHERE project=? AND mgxjob_id=?";
            PreparedStatement stmt = jobqueue.prepareStatement(sql);
            stmt.setString(1, job.getProjectName());
            stmt.setLong(2, job.getProjectJobID());
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        }
    }

    public JobI nextJob() {
        int queueId = -1;
        String projName = null;
        long mgxJobId = -1;
        JobI job = null;
        String sql = "SELECT id, project, mgxjob_id FROM jobqueue ORDER BY priority ASC LIMIT 1";
        try {
            PreparedStatement stmt = jobqueue.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                queueId = rs.getInt(1);
                projName = rs.getString(2);
                mgxJobId = rs.getLong(3);
            }
            rs.close();
            stmt.close();

            if ((queueId != -1) && (projName != null) && (mgxJobId != -1)) {
                job = new MGXJob(dispatcher, config, projName, mgxJobId);
                job.setQueueID(queueId);
            }

            // delete the job from the dispatcher queue
            if (job != null) {
                removeJob(job);
            }
        } catch (SQLException | MGXDispatcherException ex) {
        }

        return job;
    }

    public int size() {
        int ret = 0;
        try {
            Statement stmt = jobqueue.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(id) FROM jobqueue");
            ret = rs.getInt(1);
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        }
        return ret;
    }

    private void createJobQueue() throws SQLException {
        DatabaseMetaData dbm = jobqueue.getMetaData();
        ResultSet rs = dbm.getTables(null, null, "jobqueue", null);
        if (rs.next()) {
            // Table exists
        } else {
            // Table does not exist
            String sql = "CREATE TABLE jobqueue ("
                    + "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    + "   project TEXT,"
                    + "   mgxjob_id LONG,"
                    + "   priority INTEGER)";
            jobqueue.createStatement().execute(sql);
        }
    }
}
