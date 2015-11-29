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
    private final static String MGX_DATASOURCE_TYPE = "MGX";

    @EJB
    Dispatcher dispatcher;
    @EJB
    DispatcherConfiguration config;
    @EJB
    GPMSHelper gpms;
    @EJB
    FactoryHolder holder;

    private final Properties props = new Properties();
    private final ConnectionProviderI cp = new MGXConnectionProvider();

    @PostConstruct
    public void init() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        InputStream cfg = getClass().getClassLoader().getResourceAsStream("de/cebitec/mgx/dispatcher/mgx/config.properties");
        try {
            props.load(cfg);
            cfg.close();
        } catch (IOException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        try {
            // register self
            holder.registerFactory(MGX, this);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void shutdown() {
        holder.unregisterFactory(MGX);
    }

    @Override
    public JobI createJob(String projName, long jobId) throws MGXDispatcherException {
        return new MGXJob(dispatcher, config.getConveyorExecutable(), config.getValidatorExecutable(),
                getMGXPersistentDir(), cp, projName, jobId);
    }

    private String getMGXUser() {
        return props.getProperty("mgx_user");
    }

    private String getMGXPassword() {
        return props.getProperty("mgx_password");
    }

    private String getMGXPersistentDir() {
        return props.getProperty("mgx_persistent_dir");
    }

    public interface ConnectionProviderI {

        Connection getProjectConnection(String projName) throws MGXDispatcherException;
    }

    private class MGXConnectionProvider implements ConnectionProviderI {

        @Override
        public Connection getProjectConnection(String projName) throws MGXDispatcherException {

            Connection c = null;
            try {
                String url = gpms.getJDBCURLforProject(projName, MGX_DATASOURCE_TYPE);
                c = DriverManager.getConnection(url, getMGXUser(), getMGXPassword());
            } catch (SQLException ex) {
                throw new MGXDispatcherException(ex);
            }
            return c;
        }
    }

}
