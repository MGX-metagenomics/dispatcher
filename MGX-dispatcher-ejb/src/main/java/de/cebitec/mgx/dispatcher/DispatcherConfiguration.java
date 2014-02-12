package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.DispatcherConfigBase;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        } catch (IOException ex) {
            //throw new MGXDispatcherException(ex);
        }

        authToken = UUID.randomUUID();
        try {
            // write dispatcher host file
            writeDispatcherHostFile();
        } catch (IOException ex) {
            Logger.getLogger(DispatcherConfiguration.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void close() {
        // remove dispatcher host file
        File f = new File(dispatcherHostFile);
        f.delete();
    }

    public String getJobQueueFilename() {
        return config.getProperty("jobqueue_queuefile");
    }

    public String getJobQueueDriverClass() {
        return config.getProperty("jobqueue_driverclass");
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

    public UUID getAuthToken() {
        return authToken;
    }

    private void writeDispatcherHostFile() throws UnknownHostException, IOException {
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        Properties p = new Properties();
        p.put("mgx_dispatcherhost", hostname);
        p.put("mgx_dispatchertoken", authToken.toString());

        try (FileOutputStream fos = new FileOutputStream(dispatcherHostFile)) {
            p.store(fos, null);
        }
    }
}
