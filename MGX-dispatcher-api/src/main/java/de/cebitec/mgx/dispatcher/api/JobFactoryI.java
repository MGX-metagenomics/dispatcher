/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cebitec.mgx.dispatcher.api;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;


/**
 *
 * @author sjaenick
 */
public interface JobFactoryI {

    public JobI createJob(DispatcherI dispatcher, String projName, long jobId) throws MGXDispatcherException;
}
