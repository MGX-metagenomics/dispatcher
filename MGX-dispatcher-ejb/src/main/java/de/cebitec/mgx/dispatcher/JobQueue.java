package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class JobQueue {

    @EJB
    protected DispatcherConfiguration config;
    @EJB
    protected Dispatcher dispatcher;
    @EJB
    FactoryHolder factories;

    protected Connection jobqueue = null;
    //
    private final static Logger logger = Logger.getLogger(JobQueue.class.getPackage().getName());

    @PostConstruct
    public void init() {
        try {
            Class.forName(config.getJobQueueDriverClass());
            jobqueue = DriverManager.getConnection("jdbc:sqlite:" + config.getJobQueueFilename());
            createJobQueue();
        } catch (ClassNotFoundException | SQLException ex) {
            log(ex.getMessage());
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
        String sql = "INSERT INTO jobqueue (project, projectClass, projectJobID, priority) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
            stmt.setString(1, job.getProjectName());
            stmt.setString(2, job.getProjectClass());
            stmt.setLong(3, job.getProjectJobID());
            stmt.setInt(4, job.getPriority());
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }

        try (PreparedStatement stmt2 = jobqueue.prepareStatement("SELECT last_insert_rowid()")) {
            try (ResultSet rs = stmt2.executeQuery()) {
                while (rs.next()) {
                    queueId = rs.getInt(1);
                }
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }

        if (queueId != -1) {
            job.setQueueID(queueId);
            return queueId;
        }
        throw new MGXDispatcherException("No queue ID returned.");
    }

    public boolean deleteJob(JobI job) {
        String sql = "DELETE FROM jobqueue WHERE project=? AND projectJobID=?";
        try (PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
            stmt.setString(1, job.getProjectName());
            stmt.setLong(2, job.getProjectJobID());
            int numRows = stmt.executeUpdate();
            if (numRows == 1) {
                log("Job ID " + job.getProjectJobID() + " (" + job.getProjectName() + ") deleted from queue.");
                return true;
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
            return false;
        }
        return false;
    }

    public JobI nextJob() {
        int queueId = -1;

        String projName = null;
        String projClass = null;
        long projectJobId = -1;

        String sql = "SELECT id, project, projectClass, projectJobID FROM jobqueue ORDER BY priority ASC LIMIT 1";
        try (PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    queueId = rs.getInt(1);
                    projName = rs.getString(2);
                    projClass = rs.getString(3);
                    projectJobId = rs.getLong(4);
                }
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
        }

        JobI job = null;
        if ((queueId != -1) && (projName != null) && (projectJobId != -1)) {
            JobFactoryI fact = factories.getFactory(projClass);
            if (fact != null) {
                job = fact.createJob(projName, projectJobId);
                job.setQueueID(queueId);
            } else {
                log("No job factory found for project class " + projClass);
            }
        }

        // delete the job from the dispatcher queue
        if (job != null) {
            String sql2 = "DELETE FROM jobqueue WHERE project=? AND projectJobID=?";
            try (PreparedStatement stmt = jobqueue.prepareStatement(sql2)) {
                stmt.setString(1, job.getProjectName());
                stmt.setLong(2, job.getProjectJobID());
                stmt.executeUpdate();
            } catch (SQLException ex) {
                log(ex.getMessage());
            }
        }

        return job;
    }

    public int size() {
        int ret = 0;
        try (Statement stmt = jobqueue.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(id) FROM jobqueue")) {
                while (rs.next()) {
                    ret = rs.getInt(1);
                }
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
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
                    + "   projectClass TEXT NOT NULL,"
                    + "   project TEXT,"
                    + "   projectJobID LONG,"
                    + "   priority INTEGER)";
            jobqueue.createStatement().execute(sql);
        }
    }

    public void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }
}
