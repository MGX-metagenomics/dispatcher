package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(config.getJobQueueDriverClass());
        jobqueue = DriverManager.getConnection("jdbc:sqlite:" + config.getJobQueueFilename());
        createJobQueue();
    }

    @PreDestroy
    public void close() throws SQLException {
        jobqueue.close();
    }

    public Integer createJob(MGXJob job) throws MGXDispatcherException {

        /* 
         * create new job entry in the dispatcher queue
         */
        Integer queueId = null;
        try {
            String sql = "INSERT INTO jobqueue (project, mgxjob_id, priority) VALUES (\"%s\", %s, %d)";
            sql = String.format(sql, job.getProjectName(), job.getMgxJobId(), job.getPriority());
            Statement stmt = jobqueue.createStatement();
            stmt.executeUpdate(sql);
            stmt = jobqueue.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT last_insert_rowid()");
            if (rs.next()) {
                queueId = rs.getInt(1);
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }
        job.setQueueId(queueId);
        return queueId;
    }

    public void removeJob(MGXJob job) {
        try {
            String sql = String.format("DELETE FROM jobqueue WHERE project=\"%s\" AND mgxjob_id=%s", job.getProjectName(), job.getMgxJobId().toString());
            Statement stmt = jobqueue.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException ex) {
            dispatcher.log(ex.getMessage());
        }
    }

    public MGXJob nextJob() {
        Integer queueId = null;
        String projName = null;
        Long mgxJobId = null;
        MGXJob job = null;
        String sql = "SELECT id, project, mgxjob_id FROM jobqueue ORDER BY priority ASC LIMIT 1";
        try {
            Statement stmt = jobqueue.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                queueId = rs.getInt(1);
                projName = rs.getString(2);
                mgxJobId = rs.getLong(3);
            }
            rs.close();
            stmt.close();

            if ((queueId != null) && (projName != null) && (mgxJobId != null)) {
                job = new MGXJob(dispatcher, projName, mgxJobId);
                job.setQueueId(queueId);
            }

            // delete the job from the dispatcher queue
            if (job != null) {
                removeJob(job);
            }
        } catch (Exception ex) {
        }

        return job;
    }

    public int NumEntries() {
        int ret = 0;
        try {
            String sql = "SELECT COUNT(id) FROM jobqueue";
            Statement stmt = jobqueue.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
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
            String sql = new StringBuffer().append("CREATE TABLE jobqueue (").append("   id INTEGER PRIMARY KEY AUTOINCREMENT,").append("   project TEXT,").append("   mgxjob_id LONG,").append("   priority INTEGER").append(")").toString();
            jobqueue.createStatement().execute(sql);
        }
    }
}
