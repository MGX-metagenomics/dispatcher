package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author sjaenick
 */
public class GPMSHelper {

    protected DispatcherConfiguration config;
    protected Dispatcher dispatcher;
    //

    public GPMSHelper(Dispatcher disp) {
        dispatcher = disp;
        config = disp.getConfig();
    }

    public String getJDBCURLforProject(String projName) throws MGXDispatcherException {

        Connection gpmsconn = getGPMSConnection();

        String sql = new StringBuffer("select CONCAT('jdbc:', LOWER(DBMS_Type.name), '://', Host.hostname, ':',")
                .append("Host.port, '/', DataSource.name) as jdbc ")
                .append("from Project left join Project_datasources on (Project._id = Project_datasources._parent_id)")
                .append("      left join DataSource on (Project_datasources._array_value = DataSource._id)")
                .append("      left join DataSource_Type on (DataSource.datasource_type_id = DataSource_Type._id)")
                .append("      left join DataSource_DB on (DataSource._id = DataSource_DB._parent_id)")
                .append("      left join Host on (DataSource_DB.host_id = Host._id)")
                .append("      left join DBMS_Type on (DataSource_DB.dbms_type_id = DBMS_Type._id)")
                .append("where Project.name =\"%s\"")
                .toString();
        sql = String.format(sql, projName);
        String jdbc = null;
        try {
            Statement stmt = gpmsconn.createStatement();
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                jdbc = res.getString(1);
            }
            res.close();
            stmt.close();
            gpmsconn.close();
        } catch (SQLException e) {
            dispatcher.log(e.getMessage());
        }
        return jdbc;
    }

    private Connection getGPMSConnection() {
        Connection c = null;

        try {
            Class.forName(config.getGPMSDriverClass());
            c = DriverManager.getConnection(config.getGPMSURL(), config.getGPMSUser(), config.getGPMSPassword());
        } catch (Exception ex) {
            dispatcher.log(ex.getMessage());
        }
        return c;
    }
}
