import java.sql.*;
import simpledb.remote.SimpleDriver;

public class TestQueries1 {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            int z1;
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            // Step 2: execute the query
            Statement stmt = conn.createStatement();

            String qry = "select FirstName, LastName "
                       + "from DRIVER ";
            ResultSet rs = stmt.executeQuery(qry);

            //String qry2 = "select First"

            // Step 3: loop through the result set
            while (rs.next()) {
                String fname = rs.getString("FirstName");
                String lname = rs.getString("LastName");
                System.out.println(fname + " " + lname);
            }
            rs.close();
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
