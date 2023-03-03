/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package de.cebitec.mgx.dispatcher.api;

import java.util.UUID;

/**
 *
 * @author sj
 */
public interface DispatcherConfigurationI {

    UUID getAuthToken();

    String getCWLExecutable();

    String getConveyorExecutable();

    String getJobQueueDriverClass();

    String getJobQueueFilename();

    int getMaxJobs();

    String getValidatorExecutable();
    
}
