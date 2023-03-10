/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package de.cebitec.mgx.dispatcher.api;

/**
 *
 * @author sj
 */
public interface DispatcherConfigurationI {

    String getCWLExecutable();

    String getConveyorExecutable();

    String getJobQueueDriverClass();

    String getJobQueueFilename();

    int getMaxJobs();

    String getValidatorExecutable();
    
}
