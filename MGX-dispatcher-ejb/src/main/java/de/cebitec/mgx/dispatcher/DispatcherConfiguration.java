package de.cebitec.mgx.dispatcher;

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
import javax.naming.NamingException;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class DispatcherConfiguration {

    protected Properties config;
    private UUID authToken;

    /*
     * location of the host file which indicates where the dispatcher instance is running
     */
    protected static final String dispatcherHostFile = "/vol/mgx-data/GLOBAL/mgxdispatcher.properties";

    @PostConstruct
    public void create() {
        read();

        authToken = UUID.randomUUID();
        try {
            // write dispatcher host file
            writeDispatcherHostFile();
        } catch (IOException ex) {
            Logger.getLogger(DispatcherConfiguration.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void read() {
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
    }

    @PreDestroy
    public void close() {
        // remove dispatcher host file
        System.err.println("Removing host file: " + dispatcherHostFile);
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
        read();
        return Integer.parseInt(config.getProperty("mgx_max_parallel_jobs"));
    }

    public String getConveyorExecutable() {
        return config.getProperty("mgx_conveyor_graphrun");
    }

    public String getValidatorExecutable() {
        return config.getProperty("mgx_conveyor_graphvalidate");
    }

    public String getCWLExecutable() {
        return config.getProperty("mgx_cwltool");
    }

    public UUID getAuthToken() {
        return authToken;
    }

    private void writeDispatcherHostFile() throws UnknownHostException, IOException {
        Properties p = new Properties();
        p.put("mgx_dispatcherhost", getHostName());
        p.put("mgx_dispatchertoken", authToken.toString());

        try (FileOutputStream fos = new FileOutputStream(dispatcherHostFile)) {
            p.store(fos, null);
        }
        System.err.println("Wrote host file: " + dispatcherHostFile);
    }

    /**
     * http://stackoverflow.com/questions/7097623/need-to-perform-a-reverse-dns-lookup-of-a-particular-ip-address-in-java
     *
     */
    private static String getHostName() throws UnknownHostException {
        String[] bytes = InetAddress.getLocalHost().getHostAddress().split("\\.");
        if (bytes.length == 4) {
            try {
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
            } catch (final NamingException e) {
                // No reverse DNS that we could find, try with InetAddress
                System.out.print(""); // NO-OP
            }
        }
        return null;
    }
}
