/* Peter Christakos
   Andrew Morrison

     * This creates tables and runs queries for a car database
     * Location is in phase1/studentClient/simpledb
 */

import java.sql.*;
import simpledb.remote.SimpleDriver;

import javax.xml.transform.Result;

public class Examples {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // Step 1: connect to database server
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);

            System.out.println("****************** Create Tables *************************\n");
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

            System.out.println("\n****************** Create Queries **************************\n");

            // Step 2: execute the query
            Statement stmt1 = conn.createStatement();
            String qry1 = "select FirstName, LastName "
                    + "from DRIVER ";
            ResultSet rs1 = stmt1.executeQuery(qry1);

            // Step 3: loop through the result1 set
            System.out.println("Print first and last names of all drivers:");
            while (rs1.next()) {
                String fname = rs1.getString("FirstName");
                String lname = rs1.getString("LastName");
                System.out.println(fname + " " + lname);
            }
            rs1.close();

            // Step 2: execute the query
            Statement stmt2 = conn.createStatement();
            String qry2 = "select DriverID, FirstName, CarID "
                    + "from ACCIDENT, DRIVER "
                    + "where DID = DriverID ";

            ResultSet rs2 = stmt2.executeQuery(qry2);
            System.out.println("List DriverID's, FirstNames, and CarID's of Drivers who got in accidents:");
            // Step 3: loop through the result1 set
            while (rs2.next()) {
                int driverid = rs2.getInt("DriverID");
                int carid = rs2.getInt("CarID");
                String fname = rs2.getString("FirstName");
                System.out.println("DriverID: " + driverid + ", " + "FirstName: " + fname + ", " + "CarID: " + carid);
            }
            rs2.close();
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
