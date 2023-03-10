package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.JobQueueI;
import de.cebitec.mgx.dispatcher.api.DispatcherConfigurationI;
import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.api.FactoryHolderI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class JobQueue implements JobQueueI {

    @EJB
    protected DispatcherConfigurationI config;
    @EJB
    protected DispatcherI dispatcher;
    @EJB
    protected FactoryHolderI factories;

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
    public void close() {
        try {
            jobqueue.close();
        } catch (SQLException ex) {
            Logger.getLogger(JobQueue.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int createJob(JobI job) throws MGXDispatcherException {

        /*
         * create new job entry in the dispatcher queue
         */
        int queueId = -1;
        String sql = "INSERT INTO jobqueue (project, projectClass, projectJobID, priority) VALUES (?, ?, ?, ?)";
        try ( PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
            stmt.setString(1, job.getProjectName());
            stmt.setString(2, job.getProjectClass());
            stmt.setLong(3, job.getProjectJobID());
            stmt.setInt(4, job.getPriority());
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }

        try ( PreparedStatement stmt2 = jobqueue.prepareStatement("SELECT last_insert_rowid()")) {
            try ( ResultSet rs = stmt2.executeQuery()) {
                while (rs.next()) {
                    queueId = rs.getInt(1);
                }
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
            throw new MGXDispatcherException(ex.getMessage());
        }

        if (queueId != -1) {
            //job.setQueueID(queueId);
            return queueId;
        }
        throw new MGXDispatcherException("No queue ID returned.");
    }

    @Override
    public boolean deleteJob(JobI job) {
        String sql = "DELETE FROM jobqueue WHERE project=? AND projectJobID=?";
        try ( PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
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

    @Override
    public synchronized JobI nextJob() throws MGXDispatcherException {

        JobI job = null;

        String sql = "SELECT project, projectClass, projectJobID FROM jobqueue ORDER BY priority DESC";
        try ( PreparedStatement stmt = jobqueue.prepareStatement(sql)) {
            try ( ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String projName = rs.getString(1);
                    String projClass = rs.getString(2);
                    long projectJobId = rs.getLong(3);

                    if (factories.supported(projClass)) {
                        job = factories.createJob(dispatcher, projClass, projName, projectJobId);
                        break;
                    } else {
                        log("Unsupported project class " + projClass + ", skipping job " + projectJobId + " in project " + projName);
                    }
                }
            }
        } catch (SQLException ex) {
            log(ex.getMessage());
        }

        if (job != null) {
            // delete dispatcher queue entry
            String sql2 = "DELETE FROM jobqueue WHERE project=? AND projectJobID=?";
            try ( PreparedStatement stmt2 = jobqueue.prepareStatement(sql2)) {
                stmt2.setString(1, job.getProjectName());
                stmt2.setLong(2, job.getProjectJobID());
                stmt2.executeUpdate();
            } catch (SQLException ex) {
                log(ex.getMessage());
                return null;
            }
        }

        return job;
    }

    @Override
    public int size() {
        int ret = 0;
        try ( Statement stmt = jobqueue.createStatement()) {
            try ( ResultSet rs = stmt.executeQuery("SELECT COUNT(id) FROM jobqueue")) {
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
        try ( ResultSet rs = dbm.getTables(null, null, "jobqueue", null)) {
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
    }

    private void log(String msg) {
        logger.log(Level.INFO, msg);
    }
}
