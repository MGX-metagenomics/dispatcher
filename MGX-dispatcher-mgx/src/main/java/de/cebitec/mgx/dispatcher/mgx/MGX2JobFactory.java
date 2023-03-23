package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.gpms.util.GPMSDataLoaderI;
import de.cebitec.mgx.dispatcher.api.DispatcherConfigurationI;
import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.FactoryHolderI;
import de.cebitec.mgx.dispatcher.api.JobFactoryI;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class MGX2JobFactory implements JobFactoryI {

    private final static String MGX2 = "MGX-2";

    @EJB
    DispatcherConfigurationI config;
    @EJB
    GPMSDataLoaderI loader;
    @EJB
    FactoryHolderI holder;

    private final Properties props = new Properties();
    private final ConnectionProviderI cp = new MGX2ConnectionProvider();

    @PostConstruct
    public void init() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MGX2JobFactory.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        InputStream cfg = getClass().getClassLoader().getResourceAsStream("de/cebitec/mgx/dispatcher/mgx/config.properties");
        try {
            props.load(cfg);
            cfg.close();
        } catch (IOException ex) {
            Logger.getLogger(MGX2JobFactory.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        try {
            // register self
            holder.registerFactory(MGX2, this);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGX2JobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void shutdown() {
        holder.unregisterFactory(MGX2);
    }

    private final static String GETWORKFLOW = "SELECT t.file FROM job j LEFT JOIN tool t ON (j.tool_id=t.id) where j.id=?";

    @Override
    public JobI createJob(DispatcherI dispatcher, String projName, long jobId) throws MGXDispatcherException {
        String workflowFile = null;
        try ( Connection conn = cp.getProjectConnection(loader, projName)) {
            try ( PreparedStatement stmt = conn.prepareStatement(GETWORKFLOW)) {
                stmt.setLong(1, jobId);
                try ( ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        Logger.getLogger(MGX2JobFactory.class.getName()).log(Level.SEVERE, "Unable to obtain job data for job {0} in project {1}.", new Object[]{jobId, projName});
                        return null;
                    }
                    workflowFile = rs.getString(1);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(MGX2JobFactory.class.getName()).log(Level.SEVERE, null, ex);
            throw new MGXDispatcherException(ex);
        }
        if (workflowFile.endsWith(".xml")) {
            return new MGX2ConveyorJob(dispatcher, config.getConveyorExecutable(), config.getValidatorExecutable(),
                    getMGXPersistentDir(), cp, loader, projName, jobId);
        } else if (workflowFile.endsWith(".cwl")) {
            return new MGX2CWLJob(dispatcher, config.getCWLExecutable(), workflowFile,
                    getMGXPersistentDir(), cp, loader, projName, jobId);
        } else {
            throw new MGXDispatcherException("Unrecognized workflow definition file: " + workflowFile + ".");
        }
    }

    private String getMGXPersistentDir() {
        return props.getProperty("mgx_persistent_dir");
    }

}
