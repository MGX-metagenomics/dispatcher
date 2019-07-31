package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.gpms.core.DataSourceI;
import de.cebitec.gpms.core.DataSource_DBI;
import de.cebitec.gpms.core.GPMSException;
import de.cebitec.gpms.core.ProjectClassI;
import de.cebitec.gpms.core.ProjectI;
import de.cebitec.gpms.core.RoleI;
import de.cebitec.gpms.db.sql.DataSourceFactory;
import de.cebitec.gpms.model.ProjectClass;
import de.cebitec.gpms.model.Role;
import de.cebitec.gpms.util.GPMSDataLoaderI;
import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.DispatcherConfiguration;
import de.cebitec.mgx.dispatcher.FactoryHolder;
import de.cebitec.mgx.dispatcher.JobFactoryI;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    private final static String MGX2 = "MGX-2";
    private final static String MGX_DATASOURCE_TYPE = "MGX";
    private final static String MGX2_DATASOURCE_TYPE = "MGX-2";
    //
    private final static ProjectClassI mgxClass = new ProjectClass("MGX");
    //private final static ProjectClassI mgx2Class = new ProjectClass("MGX-2");
    private final static RoleI mgxUser = new Role(mgxClass, "User");
    //private final static RoleI mgx2User = new Role(mgx2Class, "User");

    @EJB
    Dispatcher dispatcher;
    @EJB
    DispatcherConfiguration config;
    @EJB
    GPMSDataLoaderI loader;
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
            holder.registerFactory(MGX2, this);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void shutdown() {
        holder.unregisterFactory(MGX);
        holder.unregisterFactory(MGX2);
    }

    private final static String GETWORKFLOW = "select t.file from job j left join tool t on (j.tool_id=t.id) where j.id=?";

    @Override
    public JobI createJob(String projName, long jobId) throws MGXDispatcherException {
        String workflowFile = null;
        try (Connection conn = cp.getProjectConnection(projName)) {
            try (PreparedStatement stmt = conn.prepareStatement(GETWORKFLOW)) {
                stmt.setLong(1, jobId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new MGXDispatcherException("Unable to access tool data.");
                    }
                    workflowFile = rs.getString(1);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
            throw new MGXDispatcherException(ex);
        }
        if (workflowFile.endsWith(".xml")) {
            return new MGXJob(dispatcher, config.getConveyorExecutable(), config.getValidatorExecutable(),
                    getMGXPersistentDir(), cp, projName, jobId);
        } else if (workflowFile.endsWith(".cwl")) {
            return new MGXCWLJob(dispatcher, config.getCWLExecutable(), workflowFile, 
                    getMGXPersistentDir(), cp, projName, jobId);
        } else {
            throw new MGXDispatcherException("Unrecognized workflow definition file: " + workflowFile + ".");
        }
    }
    //    private String getMGXUser() {
    //        return props.getProperty("mgx_user");
    //    }
    //
    //    private String getMGXPassword() {
    //        return props.getProperty("mgx_password");
    //    }

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
                DataSource_DBI targetDS = null;
                ProjectI project = loader.getProject(projName);
                for (DataSourceI ds : project.getDataSources()) {
                    if (MGX_DATASOURCE_TYPE.equals(ds.getType().getName()) || MGX2_DATASOURCE_TYPE.equals(ds.getType().getName())) {
                        if (ds instanceof DataSource_DBI) {
                            targetDS = (DataSource_DBI) ds;
                            break;
                        }
                    }
                }
                if (targetDS == null) {
                    throw new MGXDispatcherException("Could not find project datasource for project " + projName);
                }

                String[] dbAuth = loader.getDatabaseCredentials(mgxUser);

                c = DataSourceFactory.createConnection(targetDS, dbAuth[0], dbAuth[1]);
            } catch (SQLException | GPMSException ex) {
                throw new MGXDispatcherException(ex);
            }
            return c;
        }
    }

}
