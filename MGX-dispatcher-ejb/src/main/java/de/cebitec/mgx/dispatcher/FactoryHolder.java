

package de.cebitec.mgx.dispatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class FactoryHolder {
    
    private final Map<String, JobFactoryI> data = new HashMap<>();
    private final static Logger logger = Logger.getLogger(FactoryHolder.class.getPackage().getName());

    public JobFactoryI getFactory(String projClass) {
        if (projClass == null) {
            logger.log(Level.INFO, "NULL project class received");
            return null;
        }
        if (!data.containsKey(projClass)) {
            logger.log(Level.INFO, "No known handler for project class {0}", projClass);
        }
        return data.get(projClass);
    }
    
    public void registerFactory(String projClass, JobFactoryI fact) {
        data.put(projClass, fact);
        logger.log(Level.INFO, "Registered handler for project class {0}", projClass);
    }
}
