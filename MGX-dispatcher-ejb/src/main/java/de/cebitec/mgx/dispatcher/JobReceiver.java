package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.DispatcherCommand;
import de.cebitec.mgx.dispatcher.common.JobReceiverI;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import javax.annotation.Resource;
import javax.ejb.Remote;
import javax.ejb.Startup;
import javax.ejb.Stateless;

/**
 *
 * @author sjaenick
 */
@Stateless
@Startup
@Remote
public class JobReceiver implements JobReceiverI {

    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/Dispatcher")
    protected Dispatcher dispatcher;
    @Resource(lookup = "java:global/MGX-dispatcher-ear/MGX-dispatcher-ejb/DispatcherConfiguration")
    private DispatcherConfiguration config;

    public JobReceiver() {
    }

    @Override
    public void submit(DispatcherCommand c, String projName, Long mgxJobId) throws MGXDispatcherException {

        MGXJob job = new MGXJob(dispatcher, config, projName, mgxJobId);

        switch (c) {
            case EXECUTE:
                dispatcher.createJob(job);
                break;
            case CANCEL:
                dispatcher.cancelJob(job);
                break;
            case DELETE:
                dispatcher.deleteJob(job);
                break;
            case SHUTDOWN:
                dispatcher.shutdown();
                break;
            default:
                dispatcher.log("Received unknown command {0}", c);
                break;
        }

    }
}
