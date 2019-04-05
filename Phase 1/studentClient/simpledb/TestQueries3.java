import java.sql.*;
import simpledb.remote.SimpleDriver;


public class TestQueries3 {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            // Step 2: execute the query
            Statement stmt = conn.createStatement();
            String qry = "select Make, Model, Year "
                    + "from CAR, ACCIDENT "
                    + "where CID = CarID ";

            ResultSet rs = stmt.executeQuery(qry);

            System.out.println("Cars that got into accidents: ");
            // Step 3: loop through the result1 set
            while (rs.next()) {
                String make = rs.getString("Make");
                String model = rs.getString("Model");
                int year = rs.getInt("Year");
                System.out.println("Make: " + make + ", Model: " + model + ", Year: " + year);
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
