package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.api.DispatcherConfigurationI;

/**
 *
 * @author sjaenick
 * 
 * Code adapted from http://javahowto.blogspot.com/2011/02/how-to-create-and-look-up-thread-pool.html
 */
public class ThreadPoolExecutorFactory {

    public static ThreadPoolExecutor createPool(DispatcherConfigurationI cfg) {
        ThreadPoolExecutor tp = ThreadPoolExecutor.getInstance();
        tp.setMaximumPoolSize(cfg.getMaxJobs());
        tp.setCorePoolSize(cfg.getMaxJobs());
        tp.prestartAllCoreThreads();
        return tp;
    }

}
