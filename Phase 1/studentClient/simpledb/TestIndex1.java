import simpledb.remote.SimpleDriver;

import java.sql.*;

public class TestIndex1 {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt1 = conn.createStatement();
			String qry = "CREATE ex INDEX didindex on DRIVER (DID)";
			System.out.println("Running ex");
			stmt1.executeUpdate(qry);
			System.out.println("Ran ex");

			Statement stmt2 = conn.createStatement();
			String qry2 = "insert into DRIVER (DID, FirstName, LastName)" +
					" values (69, Lisa, Ann) ";
			stmt2.executeUpdate(qry2);
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
