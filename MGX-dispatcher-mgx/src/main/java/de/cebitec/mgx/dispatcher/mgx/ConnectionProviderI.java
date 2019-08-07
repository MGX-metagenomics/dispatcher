/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import java.sql.Connection;

/**
 *
 * @author sj
 */
public interface ConnectionProviderI {

    Connection getProjectConnection(String projName) throws MGXDispatcherException;
    
}
