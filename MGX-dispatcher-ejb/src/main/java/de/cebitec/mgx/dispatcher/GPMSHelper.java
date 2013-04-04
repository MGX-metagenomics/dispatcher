package de.cebitec.mgx.dispatcher;

import de.cebitec.mgx.dispatcher.common.MGXDispatcherException;
import java.sql.*;

/**
 *
 * @author sjaenick
 */
public class GPMSHelper {

    protected DispatcherConfiguration config;
    protected Dispatcher dispatcher;


    public GPMSHelper(Dispatcher disp, DispatcherConfiguration cfg) {
        dispatcher = disp;
        config = cfg;
    }

    public String getJDBCURLforProject(String projName) throws MGXDispatcherException {

        Connection gpmsconn = getGPMSConnection();

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
        //dispatcher.log(sql);
        String jdbc = null;
        try {
            Statement stmt = gpmsconn.createStatement();
            //dispatcher.log("project is "+projName);
            //stmt.setString(1, projName);
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
        } catch (ClassNotFoundException | SQLException ex) {
            dispatcher.log(ex.getMessage());
        }
        return c;
    }
}
