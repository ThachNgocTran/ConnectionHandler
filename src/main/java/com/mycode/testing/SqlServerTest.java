package com.mycode.testing;

import com.google.common.collect.Lists;
import com.mycode.sqlserver.DbUtil;
import com.mycode.sqlserver.SqlServerHandler;
import org.apache.http.util.Args;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SqlServerTest {

    /*
    *** Context ***
    Supposed in SQL Server, we have a table named "mytable", in which there are 3 columns:
    id | field1 | field 2
    We have certain equalling conditions for field1 and field2.
    We want to get all ids that match!
    *** Func call ***
    testGetIdsByField("abc", 45)
    Return the list of all ids satisfying the conditions.
     */
    public static List<String> testGetIdsByField(String conditionField1, int conditionField2) throws PropertyVetoException, SQLException {

        Args.notEmpty(conditionField1, "conditionField");

        List<String> res = Lists.newArrayList();

        Connection conn = null;
        PreparedStatement preStat = null;
        ResultSet rs = null;

        String sql = "SELECT * FROM mytable where field1 = ? AND field2 = ?";   // prevent SQL injection!

        try{

            conn = SqlServerHandler.getConnection();
            preStat = conn.prepareStatement(sql);
            preStat.setString(1, conditionField1);
            preStat.setInt(2, conditionField2);

            rs = preStat.executeQuery();

            while(rs.next()){
                res.add("id");
            }
        }
        finally {
            DbUtil.close(rs);
            DbUtil.close(preStat);
            DbUtil.close(conn);
        }

        return res;
    }
}
