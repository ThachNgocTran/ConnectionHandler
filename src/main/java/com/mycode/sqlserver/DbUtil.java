package com.mycode.sqlserver;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DbUtil {

    private static final String CLASS_NAME = DbUtil.class.getSimpleName();
    private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.error(String.format("Failed to close connection. [%s]", ExceptionUtils.getStackTrace(e)));
            }
        }
    }

    public static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.error(String.format("Failed to close statement. [%s]", ExceptionUtils.getStackTrace(e)));
            }
        }
    }

    public static void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOGGER.error(String.format("Failed to close resultset. [%s]", ExceptionUtils.getStackTrace(e)));
            }
        }
    }
}
