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
public interface DispatcherI {

    void cancelJob(JobI job) throws MGXDispatcherException;

    boolean createJob(JobI job) throws MGXDispatcherException;

    void deleteJob(JobI job) throws MGXDispatcherException;

    void handleExitingJob(JobI job);

    void setQueueMode(boolean qMode);

    boolean validate(JobI job);
    
    void scheduleJobs();
    
}
