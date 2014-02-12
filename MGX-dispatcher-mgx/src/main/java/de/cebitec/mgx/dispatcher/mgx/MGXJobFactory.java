package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.DispatcherConfiguration;
import de.cebitec.mgx.dispatcher.FactoryHolder;
import de.cebitec.mgx.dispatcher.JobFactoryI;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
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
public class MGXJobFactory implements JobFactoryI {

    private final static String MGX = "MGX";

    @EJB
    Dispatcher dispatcher;
    @EJB
    DispatcherConfiguration config;
    @EJB
    GPMSHelper gpms;
    @EJB
    FactoryHolder holder;

    private String conveyor;
    private final Properties props = new Properties();

    @PostConstruct
    public void init() {
        conveyor = config.getConveyorExecutable();
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }

        InputStream cfg = getClass().getClassLoader().getResourceAsStream("de/cebitec/mgx/dispatcher/mgx/config.properties");
        try {
            props.load(cfg);
        } catch (IOException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            cfg.close();
        } catch (IOException ex) {
        }
        // register self
        holder.registerFactory(MGX, this);
    }

    @PreDestroy
    public void shutdown() {
        holder.unregisterFactory(MGX);
    }

    @Override
    public JobI createJob(String projName, long jobId) {

        try {
            return new MGXJob(dispatcher, conveyor, config.getValidatorExecutable(), getMGXPersistentDir(), new MGXConnectionProvider(), projName, jobId);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public String getMGXUser() {
        return props.getProperty("mgx_user");
    }

    public String getMGXPassword() {
        return props.getProperty("mgx_password");
    }

    public String getMGXPersistentDir() {
        return props.getProperty("mgx_persistent_dir");
    }

    public interface ConnectionProviderI {

        Connection getProjectConnection(String projName);
    }

    private class MGXConnectionProvider implements ConnectionProviderI {

        @Override
        public Connection getProjectConnection(String projName) {

            Connection c = null;
            try {
                String url = gpms.getJDBCURLforProject(projName);
                c = DriverManager.getConnection(url, getMGXUser(), getMGXPassword());
            } catch (SQLException | MGXDispatcherException ex) {
                Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            assert c != null;
            return c;
        }
    }

}