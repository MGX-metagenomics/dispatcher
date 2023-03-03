package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;

/**
 *
 * @author sj
 */
public interface FactoryHolderI {

    JobFactoryI getFactory(String projClass) throws MGXDispatcherException;

    void registerFactory(String projClass, JobFactoryI fact) throws MGXDispatcherException;

    JobFactoryI unregisterFactory(String projClass);
    
}
