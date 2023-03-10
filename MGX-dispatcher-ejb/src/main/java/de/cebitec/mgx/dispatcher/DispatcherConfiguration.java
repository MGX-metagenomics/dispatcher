package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.DispatcherConfigurationI;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.naming.NamingException;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class DispatcherConfiguration implements DispatcherConfigurationI {

    protected Properties config;
    private static final Logger LOG = Logger.getLogger(DispatcherConfiguration.class.getName());

    /*
     * location of the host file which indicates where the dispatcher instance is running
     */
    protected static final String dispatcherHostFile = "/vol/mgx-data/GLOBAL/mgxdispatcher.properties";

    @PostConstruct
    public void create() {
        read();
        try {
            // write dispatcher host file
            writeDispatcherHostFile();
        } catch (IOException | NamingException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    private void read() {
        StringBuilder cfgFile = new StringBuilder(System.getProperty("user.dir"));
        cfgFile.append(File.separator);
        cfgFile.append("mgx_dispatcher.properties");

        File cfg = new File(cfgFile.toString());
        if (!cfg.exists() || !cfg.canRead()) {
            throw new RuntimeException("Dispatcher configuration file " + cfgFile.toString() + " missing or unreadable.");
        }

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
    }

    @PreDestroy
    public void close() {
        // remove dispatcher host file
        LOG.info("Removing host file: " + dispatcherHostFile);
        File f = new File(dispatcherHostFile);
        f.delete();
    }

    @Override
    public String getJobQueueFilename() {
        return config.getProperty("jobqueue_queuefile");
    }

    @Override
    public String getJobQueueDriverClass() {
        return config.getProperty("jobqueue_driverclass");
    }

    @Override
    public int getMaxJobs() {
        read();
        return Integer.parseInt(config.getProperty("mgx_max_parallel_jobs"));
    }

    @Override
    public String getConveyorExecutable() {
        return config.getProperty("mgx_conveyor_graphrun");
    }

    @Override
    public String getValidatorExecutable() {
        return config.getProperty("mgx_conveyor_graphvalidate");
    }

    @Override
    public String getCWLExecutable() {
        return config.getProperty("mgx_cwltool");
    }

    private void writeDispatcherHostFile() throws UnknownHostException, IOException, NamingException {

        String hostName = getHostName();
        if (hostName == null || hostName.isEmpty()) {
            throw new RuntimeException("Unable to determine own host name.");
        }

        Properties p = new Properties();
        p.put("mgx_dispatcherhost", hostName);

        File hostFile = new File(dispatcherHostFile);
        File hostFileDir = hostFile.getParentFile();
        if (!hostFileDir.canWrite()) {
            LOG.log(Level.SEVERE, "No write permission to {0}", hostFileDir.getAbsolutePath());
            throw new RuntimeException("Unable to create dispatcher host file.");
        }
        
        if (hostFile.exists() && !hostFile.canWrite()) {
            LOG.severe("No write permission to existing " + dispatcherHostFile);
            throw new RuntimeException("Unable to overwrite dispatcher host file.");
        }

        try ( FileOutputStream fos = new FileOutputStream(dispatcherHostFile)) {
            p.store(fos, null);
        }

        LOG.info("Wrote host file: " + dispatcherHostFile);
    }

    /**
     * http://stackoverflow.com/questions/7097623/need-to-perform-a-reverse-dns-lookup-of-a-particular-ip-address-in-java
     *
     */
    private static String getHostName() throws UnknownHostException, NamingException {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();

        String[] bytes = hostAddress.split("\\.");
        if (bytes.length == 4) {
            final Properties env = new Properties();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            final javax.naming.directory.DirContext ctx = new javax.naming.directory.InitialDirContext(env);
            final String reverseDnsDomain = bytes[3] + "." + bytes[2] + "." + bytes[1] + "." + bytes[0] + ".in-addr.arpa";
            final javax.naming.directory.Attributes attrs = ctx.getAttributes(reverseDnsDomain, new String[]{"PTR",});
            for (final javax.naming.NamingEnumeration<? extends javax.naming.directory.Attribute> ae = attrs.getAll(); ae.hasMoreElements();) {
                final javax.naming.directory.Attribute attr = ae.next();
                final String attrId = attr.getID();
                for (final java.util.Enumeration<?> vals = attr.getAll(); vals.hasMoreElements();) {
                    String value = vals.nextElement().toString();
                    if ("PTR".equals(attrId)) {
                        final int len = value.length();
                        if (value.charAt(len - 1) == '.') {
                            value = value.substring(0, len - 1);
                        }
                        return value;
                    }
                }
            }
            ctx.close();
        }
        return null;
    }
}
