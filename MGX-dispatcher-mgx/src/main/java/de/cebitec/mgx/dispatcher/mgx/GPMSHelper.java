package de.cebitec.mgx.dispatcher.mgx;

import de.cebitec.mgx.dispatcher.DispatcherConfiguration;
import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.sql.DataSource;

/**
 *
 * @author sjaenick
 */
@Singleton
@Startup
public class GPMSHelper {

    @EJB
    DispatcherConfiguration config;
    //
    @Resource(mappedName = "jdbc/GPMS")
    private DataSource gpmsds;

    private final static Logger logger = Logger.getLogger(GPMSHelper.class.getPackage().getName());

    public String getJDBCURLforProject(String projName) throws MGXDispatcherException {

        Connection gpmsconn;  
        try {
            gpmsconn = gpmsds.getConnection(); // getGPMSConnection(config);
        } catch (SQLException ex) {
            throw new MGXDispatcherException(ex);
        }

        String sql = "SELECT CONCAT('jdbc:', LOWER(DBMS_Type.name), '://', Host.hostname, ':',"
                + "Host.port, '/', DataSource.name) as jdbc "
                + "FROM Project LEFT JOIN Project_datasources on (Project._id = Project_datasources._parent_id)"
                + "      left join DataSource on (Project_datasources._array_value = DataSource._id)"
                + "      left join DataSource_Type on (DataSource.datasource_type_id = DataSource_Type._id)"
                + "      left join DataSource_DB on (DataSource._id = DataSource_DB._parent_id)"
                + "      left join Host on (DataSource_DB.host_id = Host._id)"
                + "      left join DBMS_Type on (DataSource_DB.dbms_type_id = DBMS_Type._id) "
                + "WHERE Project.name=\"%s\"";
        // + "WHERE Project.name=?";
        sql = String.format(sql, projName);
        String jdbc = null;
        try (Statement stmt = gpmsconn.createStatement()) {
            try (ResultSet res = stmt.executeQuery(sql)) {
                while (res.next()) {
                    jdbc = res.getString(1);
                }
            }
        } catch (SQLException e) {
            log(e.getMessage());
        } finally {
            try {
                gpmsconn.close();
            } catch (SQLException ex) {
            }
        }
        return jdbc;
    }

//    private Connection getGPMSConnection(DispatcherConfiguration config) {
//        Connection c = null;
//
//        try {
//            Class.forName(config.getGPMSDriverClass());
//            c = DriverManager.getConnection(config.getGPMSURL(), config.getGPMSUser(), config.getGPMSPassword());
//        } catch (ClassNotFoundException | SQLException ex) {
//            log(ex.getMessage());
//        }
//        return c;
//    }

    private void log(String msg) {
        logger.log(Level.INFO, msg);
    }

    private void log(String msg, Object... args) {
        logger.log(Level.INFO, String.format(msg, args));
    }
}
