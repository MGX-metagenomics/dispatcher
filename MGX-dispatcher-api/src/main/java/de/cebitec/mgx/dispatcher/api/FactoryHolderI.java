package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;

/**
 *
 * @author sj
 */
public interface FactoryHolderI {

    void registerFactory(String projClass, JobFactoryI fact) throws MGXDispatcherException;

    JobFactoryI unregisterFactory(String projClass);

    JobI createJob(DispatcherI dispatcher, String projClass, String projName, long projectJobId) throws MGXDispatcherException;

    boolean available();
    
    boolean supported(String projClass);

}
