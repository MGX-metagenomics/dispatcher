package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class FactoryHolder {

    @EJB
    protected Dispatcher dispatcher;

    private final Map<String, JobFactoryI> data = new HashMap<>();
    private final static Logger logger = Logger.getLogger(FactoryHolder.class.getPackage().getName());

    public JobFactoryI getFactory(String projClass) throws MGXDispatcherException {
        if (projClass == null) {
            logger.log(Level.INFO, "NULL project class received.");
            throw new MGXDispatcherException("No project class received.");
        }
        if (!data.containsKey(projClass)) {
            logger.log(Level.INFO, "No known handler for project class {0}", projClass);
            throw new MGXDispatcherException("No registered job factory for project class " + projClass);
        }
        return data.get(projClass);
    }

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

        //
        // with a new factory registered, attempt to process queued jobs
        //
        // null check required for unit testing since EJBs don't work in junit5
        if (dispatcher != null) {
            dispatcher.scheduleJobs();
        }
    }

    public JobFactoryI unregisterFactory(String projClass) {
        if (data.containsKey(projClass)) {
            logger.log(Level.INFO, "Unregistered handler for project class {0}", projClass);
            return data.remove(projClass);
        }
        return null;
    }
}
