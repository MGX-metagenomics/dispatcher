package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.DispatcherConfigBase;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class DispatcherConfiguration extends DispatcherConfigBase {

    protected Properties config;
    private UUID authToken;

    @PostConstruct
    public void create() {
        StringBuilder cfgFile = new StringBuilder(System.getProperty("user.dir"));
        cfgFile.append(File.separator);
        cfgFile.append("mgx_dispatcher.properties");

        // load dispatcher configuration
        config = new Properties();
        FileInputStream in;
        try {
            in = new FileInputStream(cfgFile.toString());
            config.load(in);
            in.close();
        } catch (Exception ex) {
            //throw new MGXDispatcherException(ex);
        }
        
        authToken = UUID.randomUUID();

        // write dispatcher host file
        writeDispatcherHostFile();
    }

    @PreDestroy
    public void close() {
        // remove dispatcher host file
        File f = new File(dispatcherHostFile);
        f.delete();
    }

    public String getGPMSUser() {
        return config.getProperty("gpms_user");
    }

    public String getGPMSPassword() {
        return config.getProperty("gpms_password");
    }

    public String getGPMSURL() {
        return config.getProperty("gpms_jdbc_url");
    }

    public String getGPMSDriverClass() {
        return config.getProperty("gpms_driverclass");
    }

    public String getJobQueueFilename() {
        return config.getProperty("jobqueue_queuefile");
    }

    public String getJobQueueDriverClass() {
        return config.getProperty("jobqueue_driverclass");
    }

    public String getMGXDriverClass() {
        return config.getProperty("mgx_driverclass");
    }

    public String getMGXUser() {
        return config.getProperty("mgx_user");
    }

    public String getMGXPassword() {
        return config.getProperty("mgx_password");
    }

    public int getMaxJobs() {
        return Integer.parseInt(config.getProperty("mgx_max_parallel_jobs"));
    }

    public String getConveyorExecutable() {
        return config.getProperty("mgx_conveyor_graphrun");
    }

    public String getValidatorExecutable() {
        return config.getProperty("mgx_conveyor_graphvalidate");
    }

    public String getMGXPersistentDir() {
        return config.getProperty("mgx_persistent_dir");
    }
    
    public UUID getAuthToken() {
        return authToken;
    }

    private void writeDispatcherHostFile()  {
        String hostname = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
           // throw new MGXDispatcherException(ex);
        }


        Properties p = new Properties();
        p.put("mgx_dispatcherhost", hostname);
        p.put("mgx_dispatchertoken", authToken.toString());

        try {
            FileOutputStream fos = new FileOutputStream(dispatcherHostFile);
            p.store(fos, null);
            fos.close();
        } catch (IOException ex) {
        }
    }
}
