import java.sql.*;
import simpledb.remote.SimpleDriver;

public class CreateTestDB {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();
            String s = "create table CAR(CID int, Make varchar(10), Model varchar(10), Year int)";
            stmt.executeUpdate(s);
            System.out.println("Table CAR created.");

            s = "insert into CAR(CID, Make, Model, Year) values ";
            String[] carVals =
                   {"(1, 'Mercedes', 'C100', 2001)",
                    "(2, 'Ford', 'Trailblazer', 2020)",
                    "(3, 'Range Rover', 'Sport', 2011)",
                    "(4, 'Rolls Royce', 'Phantom', 2020)",
                    "(5, 'SmartCar', 'extraSmart', 1969)",
                    "(6, 'BMW', 'i8', 2018)",
                    "(7, 'Jaguar', 'F-Pace', 2017)",
                    "(8, 'Kia', 'Amanti', 2004)",
                    "(9, 'Chrysler', 'Sebring', 2004)"};
            for (int i=0; i<carVals.length; i++)
                stmt.executeUpdate(s + carVals[i]);
            System.out.println("DRIVER records inserted.");
            s = "create table DRIVER(DID int, FirstName varchar(8), LastName varchar(8))";
            stmt.executeUpdate(s);
            System.out.println("Table Driver created.");

            s = "insert into DRIVER(DID, FirstName, LastName) values ";
            String[] driverVals =
                   {"(20, 'Baby', 'Driver')",
                    "(10, 'Nico', 'Fabbrini')",
                    "(30, 'Dev', 'Patel')"};
            for (int i=0; i<driverVals.length; i++)
                stmt.executeUpdate(s + driverVals[i]);
            System.out.println("DRIVER records inserted.");
            s = "create table ACCIDENT(ID int, DriverID int, CarID int)";
            stmt.executeUpdate(s);
            System.out.println("Table ACCIDENT created.");

            s = "insert into ACCIDENT(ID, DriverID, CarID) values ";
            String[] accidentVals =
                   {"(12, 10, 1)",
                    "(22, 10, 2)",
                    "(32, 10, 3)",
                    "(42, 20, 2)",
                    "(52, 20, 3)",
                    "(62, 30, 4)"};
            for (int i=0; i<accidentVals.length; i++)
                stmt.executeUpdate(s + accidentVals[i]);
            System.out.println("ACCIDENT records inserted.");
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
        finally {
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
