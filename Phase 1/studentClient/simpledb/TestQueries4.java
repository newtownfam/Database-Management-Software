import simpledb.remote.SimpleDriver;

import java.sql.*;
import java.sql.*;
import simpledb.remote.SimpleDriver;

import javax.xml.transform.Result;

public class TestQueries4 {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            // Step 2: execute the query
            Statement stmt1 = conn.createStatement();
            String qry = "DELETE from Driver"
                    +    " WHERE DID = 5";
            stmt1.executeUpdate(qry);
            System.out.println("Value deleted from Driver: (5, Peter, Christakos");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Step 4: close the connection
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
