/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import java.util.UUID;

/**
 *
 * @author sj
 */
public interface JobReceiverI {

    void cancel(String projClass, String projName, long projectJobId) throws MGXDispatcherException;

    void delete(String projClass, String projName, long projectJobId) throws MGXDispatcherException;

    boolean shutdown(UUID auth);

    boolean submit(String projClass, String projName, long projectJobId) throws MGXDispatcherException;

    boolean validate(String projClass, String projName, long projectJobId);
    
}
