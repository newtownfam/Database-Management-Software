import java.sql.*;
import simpledb.remote.SimpleDriver;


public class TestQueries2 {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            // Step 2: execute the query
            Statement stmt = conn.createStatement();
            String qry = "select DriverID, FirstName, CarID "
                       + "from ACCIDENT, DRIVER "
                       + "where DID = DriverID ";

            ResultSet rs = stmt.executeQuery(qry);
            System.out.println("List DriverID's, FirstNames, and CarID's of Drivers who got in accidents");
            // Step 3: loop through the result1 set
            while (rs.next()) {
                int driverid = rs.getInt("DriverID");
                int carid = rs.getInt("CarID");
                String fname = rs.getString("FirstName");
                System.out.println("DriverID: " + driverid + ", " + "FirstName: " + fname + ", " + "CarID: " + carid);
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
