/******************************************************************/
import java.sql.*;
import java.util.Random;
import simpledb.remote.SimpleDriver;
	public class CreateTestTables {
		final static int maxSize=100;
		/**
		 * @param args
		 */
		public static void main(String[] args) {
			// TODO Auto-generated method stub
			Connection conn=null;
			Driver d = new SimpleDriver();
			String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
			String url = "jdbc:simpledb://" + host;

			Random rand=null;
			Statement s=null;
			try {
				conn = d.connect(url, null);
				s=conn.createStatement();
				s.executeUpdate("Create table test1" +
						"( a1 int," +
						"  a2 int"+
						")");
				s.executeUpdate("Create table test2" +
						"( a1 int," +
						"  a2 int"+
						")");
				s.executeUpdate("Create table test3" +
						"( a1 int," +
						"  a2 int"+
						")");
				s.executeUpdate("Create table test4" +
						"( a1 int," +
						"  a2 int"+
						")");
				s.executeUpdate("Create table test5" +
						"( a1 int," +
						"  a2 int"+
						")");

                System.out.println("No index assigned to test 1 and test 5 tables.");
				s.executeUpdate("create sh index idx1 on test2 (a1)");
				System.out.println("Created sh index on test 2 table.");
				s.executeUpdate("create ex index idx2 on test3 (a1)");
                System.out.println("Created ex index on test 3 table.");
				s.executeUpdate("create bt index idx3 on test4 (a1)");
                System.out.println("Created bt index on test 4 table.\n");
				long totalTime = System.nanoTime();
				for(int i=1;i<6;i++)
				{
				    //System.out.println("I value: " + i);
					if(i!=5)
					{
						rand=new Random(1);// ensure every table gets the same data
						for(int j=0;j<maxSize;j++)
						{
						    long totalTime2 = System.nanoTime();
							String x =("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
							s.executeUpdate(x);
                            long estimatedTotalTIme2= System.nanoTime() - totalTime2;
							System.out.println( "Inserting " + x + " took " +  estimatedTotalTIme2);
						}
					}
					else//case where i=5
					{
						for(int j=0;j<maxSize/2;j++)// insert 10000 records into test5
						{
							s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
						}
					}
				}
				long estimatedTotal = System.nanoTime() - totalTime;
				System.out.println( "\nInserted " + maxSize + " records! Total time: " +  estimatedTotal);

                // test 1
                long totalTimeTable1 = System.nanoTime();
                String t1 = ("select a1,a2 " +
                        "from test1 " +
                        "where a1 = 10 ");
                ResultSet rs = s.executeQuery(t1);
                long estimatedTotalTable1 = System.nanoTime() - totalTimeTable1;

                while (rs.next()) {
                    int a1 = rs.getInt("a1");
                    int a2 = rs.getInt("a2");
                    System.out.println("A1 Value: " + a1 + ", A2 Value: " + a2);
                }
                rs.close();
                System.out.println("\nQuery 1: No Index ----- Stmt: " + t1 + ", Time: " + estimatedTotalTable1);

                // test 2
                long totalTimeTable2 = System.nanoTime();
                String t2 = ("select a1,a2 " +
                        "from test2 " +
                        "where a1 = 10 ");
                ResultSet rs2 = s.executeQuery(t2);
                long estimatedTotalTable2 = System.nanoTime() - totalTimeTable2;

                while (rs2.next()) {
                    int a1 = rs2.getInt("a1");
                    int a2 = rs2.getInt("a2");
                    System.out.println("A1 Value: " + a1 + ", A2 Value: " + a2);
                }
                rs.close();
                System.out.println("Query 2: Static Hash -- Stmt: " + t2 + ", Time: " + estimatedTotalTable2);

                // test 3
                long totalTimeTable3 = System.nanoTime();
                String t3 = ("select a1,a2 " +
                        "from test3 " +
                        "where a1 = 10 ");
                ResultSet rs3 = s.executeQuery(t3);
                long estimatedTotalTable3 = System.nanoTime() - totalTimeTable3;

                while (rs3.next()) {
                    int a1 = rs3.getInt("a1");
                    int a2 = rs3.getInt("a2");
                    System.out.println("A1 Value: " + a1 + ", A2 Value: " + a2);
                }
                rs3.close();
                System.out.println("Query 3: Ext Hash ----- Stmt: " + t3 + ", Time: " + estimatedTotalTable3);

                // test 4
                long totalTimeTable4 = System.nanoTime();
                String t4 = ("select a1,a2 " +
                        "from test4 " +
                        "where a1 = 10 ");
                ResultSet rs4 = s.executeQuery(t4);
                long estimatedTotalTable4 = System.nanoTime() - totalTimeTable4;

                while (rs4.next()) {
                    int a1 = rs4.getInt("a1");
                    int a2 = rs4.getInt("a2");
                    System.out.println("A1 Value: " + a1 + ", A2 Value: " + a2);
                }
                rs4.close();
                System.out.println("Query 4: B-Tree ------- Stmt: " + t4 + ", Time: " + estimatedTotalTable4);

                // test 5
                long totalTimeTable5 = System.nanoTime();
                String t5 = "select a1,a2 " +
                             "from test5, test1 " +
                             "where a1 = a1 ";
                s.executeQuery(t5);
                s.executeQuery(t5);
                long estimatedTotalTable5 = System.nanoTime() - totalTimeTable5;
                System.out.println("\nQuery 5: Merge T5 with T1 -- Stmt: " + t5 + ", Time: " + estimatedTotalTable5);

                // test 5
                long totalTimeTable6 = System.nanoTime();
                String t6 = "select a1,a2 " +
                        "from test5, test2 " +
                        "where a1 = a1 ";
                s.executeQuery(t6);
                s.executeQuery(t6);
                long estimatedTotalTable6 = System.nanoTime() - totalTimeTable6;
                System.out.println("Query 6: Merge T5 with T2 -- Stmt: " + t6 + ", Time: " + estimatedTotalTable6);

                // test 5
                long totalTimeTable7 = System.nanoTime();
                String t7 = "select a1,a2 " +
                        "from test5, test3 " +
                        "where a1 = a1 ";
                s.executeQuery(t7);
                ResultSet rs5 = s.executeQuery(t7);
                long estimatedTotalTable7 = System.nanoTime() - totalTimeTable7;
                System.out.println("Query 7: Merge T5 with T3 -- Stmt: " + t7 + ", Time: " + estimatedTotalTable7);

                // test 5
                long totalTimeTable8 = System.nanoTime();
                String t8 = "select a1,a2 " +
                        "from test5, test4 " +
                        "where a1 = a1 ";
                s.executeQuery(t8);
                long estimatedTotalTable8 = System.nanoTime() - totalTimeTable8;
                System.out.println("Query 8: Merge T5 with T4 -- Stmt: " + t8 + ", Time: " + estimatedTotalTable8);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally
			{
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
