package com.mycode.sqlserver;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

/*
Used 3rd party pooling library for SQL Server connection.
 */
public class SqlServerHandler {

    private static final String CLASS_NAME = SqlServerHandler.class.getSimpleName();
    private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // INSTANCE PART
    private ComboPooledDataSource cpds;

    private SqlServerHandler() {

    }

    private synchronized Connection createConnection() throws PropertyVetoException, SQLException {

        if (cpds == null){

            LOGGER.info("First time call. Initializing Connection Pool.");

            cpds = new ComboPooledDataSource();
            cpds.setDriverClass("DATABASE_SQL_DRIVER"); // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            cpds.setJdbcUrl("DATABASE_SQL_URL");        // "jdbc:sqlserver://[YourDb].database.windows.net:1433;databaseName=[YourDatabaseName]"
            cpds.setUser("DATABASE_SQL_SERVER_USER");
            cpds.setPassword("DATABASE_SQL_SERVER_PASSWORD");
            cpds.setTestConnectionOnCheckin(true);
            cpds.setTestConnectionOnCheckout(true);
            cpds.setIdleConnectionTestPeriod(Integer.valueOf("DATABASE_CONNECTION_TEST_PERIOD"));
        }

        Connection cnn = cpds.getConnection();

        return cnn;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STATIC PART
    private static SqlServerHandler instance = new SqlServerHandler();

    public static Connection getConnection() throws PropertyVetoException, SQLException {
        return instance.createConnection();
    }

    public static void init(){

        Connection conn = null;

        try{
            conn = getConnection();
            // Trick to force initialization
        }
        catch (Throwable th){

        }
        finally {
            DbUtil.close(conn);
        }
    }
}
