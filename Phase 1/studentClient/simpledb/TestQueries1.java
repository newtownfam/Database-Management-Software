import java.sql.*;
import simpledb.remote.SimpleDriver;

import javax.xml.transform.Result;

public class TestQueries1 {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            // Step 2: execute the query
            Statement stmt1 = conn.createStatement();
            String qry = "insert into Driver (DID, FirstName, LastName) "
                       + "values (5, Peter, Christakos)";
            stmt1.executeUpdate(qry);
            System.out.println("New value inserted to Driver: (5, Peter, Christakos");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            // Step 4: close the connection
            try {
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
