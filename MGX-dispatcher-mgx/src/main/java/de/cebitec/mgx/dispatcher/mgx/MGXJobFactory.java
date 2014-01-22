package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.dispatcher.Dispatcher;
import de.cebitec.mgx.dispatcher.DispatcherConfiguration;
import de.cebitec.mgx.dispatcher.GPMSHelper;
import de.cebitec.mgx.dispatcher.JobFactoryI;
import de.cebitec.mgx.dispatcher.JobI;
import de.cebitec.mgx.dispatcher.mgx.MGXJob;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    @EJB
    Dispatcher dispatcher;
    @EJB
    DispatcherConfiguration config;
    @EJB
    GPMSHelper gpms;

    @Override
    public JobI createJob(String projName, long jobId) {
        try {
            return new MGXJob(dispatcher, config, gpms, projName, jobId);
        } catch (MGXDispatcherException ex) {
            Logger.getLogger(MGXJobFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
