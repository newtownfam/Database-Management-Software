import java.sql.*;
import simpledb.remote.SimpleDriver;
import simpledb.buffer.*;
import simpledb.server.Startup;
import simpledb.server.Test;


public class RunAllTests {
	public static void main(String[] args) {
		System.out.println("---------------------------------------- STARTING\n\n");
		String[] args2 = {"compsci"};
		String[] args3 = {"cs4432DB"};
		try {
			Startup.main(args3);
		} catch (Exception e) {
			System.out.println("Startup exception.");
		}

		System.out.println("---------------------------------------- Creating Students DB\n\n");
		System.out.println("Students DB: \n");
		CreateStudentDB.main(args);
		FindMajors.main(args2);
		StudentMajor.main(args);
		ChangeMajor.main(args);

		System.out.println("---------------------------------------- Creating Test DB\n\n");
		System.out.println("Test DB: \n");
		CreateTestDB.main(args);
		//TestQueries1.main(args);
		TestQueries2.main(args);

		System.out.println("---------------------------------------- Advanced vs Basic statistics\n\n");
		Test.main(args);

		System.out.println("---------------------------------------- All tests finished successfully. Database still running... ");

		System.out.println("---------------------------------------- Creating an index on driver ID... ");
		TestIndex1.main(args);
	}
}
