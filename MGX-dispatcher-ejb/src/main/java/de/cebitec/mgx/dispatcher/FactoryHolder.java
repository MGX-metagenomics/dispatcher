package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.DispatcherI;
import de.cebitec.mgx.dispatcher.api.JobFactoryI;
import de.cebitec.mgx.dispatcher.api.FactoryHolderI;
import de.cebitec.mgx.dispatcher.api.JobI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class FactoryHolder implements FactoryHolderI {

    @EJB
    protected DispatcherI dispatcher;

    private final Map<String, JobFactoryI> data = new ConcurrentHashMap<>();
    private final static Logger logger = Logger.getLogger(FactoryHolder.class.getPackage().getName());

    @Override
    public boolean available() {
        return !data.isEmpty();
    }

    @Override
    public boolean supported(String projClass) {
        return data.containsKey(projClass);
    }

    @Override
    public JobI createJob(DispatcherI dispatcher, String projClass, String projName, long projectJobId) throws MGXDispatcherException {
        JobFactoryI jf = getFactory(projClass);
        if (jf != null) {
            return jf.createJob(dispatcher, projName, projectJobId);
        }
        logger.log(Level.INFO, "No job factory found for project class {0} while attempting to access project {1}", new Object[]{projClass, projName});
        return null;
    }

    private JobFactoryI getFactory(String projClass) throws MGXDispatcherException {
        if (projClass == null) {
            logger.log(Level.INFO, "NULL project class received.");
            throw new MGXDispatcherException("No project class received.");
        }
        if (!data.containsKey(projClass)) {
            logger.log(Level.INFO, "No known handler for project class {0}", projClass);
            return null;
            //throw new MGXDispatcherException("No registered job factory for project class " + projClass);
        }
        return data.get(projClass);
    }

    @Override
    public void registerFactory(String projClass, JobFactoryI fact) throws MGXDispatcherException {
        if (projClass == null || projClass.trim().isEmpty() || fact == null) {
            throw new MGXDispatcherException("Factory registration error.");
        }
        if (data.containsKey(projClass)) {
            logger.log(Level.INFO, "Replacing handler for project class {0}", projClass);
            data.put(projClass, fact);
        } else {
            data.put(projClass, fact);
            logger.log(Level.INFO, "Registered handler for project class {0}", projClass);
        }
    }

    @Override
    public JobFactoryI unregisterFactory(String projClass) {
        if (data.containsKey(projClass)) {
            logger.log(Level.INFO, "Unregistered handler for project class {0}", projClass);
            return data.remove(projClass);
        }
        return null;
    }
}
