/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.gpms.core.DataSourceI;
import de.cebitec.gpms.core.DataSource_DBI;
import de.cebitec.gpms.core.GPMSException;
import de.cebitec.gpms.core.ProjectClassI;
import de.cebitec.gpms.core.ProjectI;
import de.cebitec.gpms.core.RoleI;
import de.cebitec.gpms.db.sql.DataSourceFactory;
import de.cebitec.gpms.model.ProjectClass;
import de.cebitec.gpms.model.Role;
import de.cebitec.gpms.util.GPMSDataLoaderI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author sj
 */
public class MGXConnectionProvider implements ConnectionProviderI {
    
    private final static String MGX_DATASOURCE_TYPE = "MGX";
    private final static ProjectClassI mgxClass = new ProjectClass("MGX");
    private final static RoleI mgxUser = new Role(mgxClass, "User");

    @Override
    public Connection getProjectConnection(GPMSDataLoaderI loader, String projName) throws MGXDispatcherException {
        Connection c = null;
        try {
            DataSource_DBI targetDS = null;
            ProjectI project = loader.getProject(projName);
            for (DataSourceI ds : project.getDataSources()) {
                if (MGX_DATASOURCE_TYPE.equals(ds.getType().getName())) {
                    if (ds instanceof DataSource_DBI) {
                        targetDS = (DataSource_DBI) ds;
                        break;
                    }
                }
            }
            if (targetDS == null) {
                throw new MGXDispatcherException("Could not find project datasource for MGX project " + projName);
            }
            String[] dbAuth = loader.getDatabaseCredentials(mgxUser);
            c = DataSourceFactory.createConnection(targetDS, dbAuth[0], dbAuth[1]);
        } catch (SQLException | GPMSException ex) {
            throw new MGXDispatcherException(ex);
        }
        return c;
    }
    
}
