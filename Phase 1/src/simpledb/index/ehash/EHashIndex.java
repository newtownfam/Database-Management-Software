package simpledb.index.ehash;

import simpledb.index.hash.HashIndex;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;

public class EHashIndex implements Index{

	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;

	// schema and table scan to store information about the the global index
	private Schema globalSchema;
	private TableScan globalTableScan;

	// schema and table scan to store information about the local depth buckets
	private Schema bucketScema;
	private TableScan localTableScan;

	// information about the global schema
	private int globalDepth;
	private int bucketSize = 4;
	private int currBucket = -1;

	/**
	 * Creates an extendable hash index
	 */
	public EHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;

		/* create the global schema
		 * - fields added are for the global bucket where
		 *   - globalBucket is the id of the bucket in the global table
		 *   - bucketPointer is the pointer to the local buckets
		 *   - depth is the global depth
		 */
		globalSchema = new Schema();
		globalSchema.addIntField("globalBucket");
		globalSchema.addIntField("bucketPointer");
		globalSchema.addIntField("depth");

		/* create the local schema
		 * - fields are added to the local buckets where
		 *   - bucketID is the ID of the bucket
		 *   - capacity is how many entries the bucket can hold
		 *   - depth is the local depth
		 */
		bucketScema = new Schema();
		bucketScema.addIntField("bucketID");
		bucketScema.addIntField("capacity");
		bucketScema.addIntField("depth");

		// Initialize the table scans
		initGlobalTableScan();
		initLocalTableScan();

		// set the global depth to 1 if not initialized
		globalTableScan.beforeFirst();
		if(!globalTableScan.next()) {
			globalDepth = 1;
			globalTableScan.setInt("depth", globalDepth);
		} else {
			globalDepth = globalTableScan.getInt("depth");
		}

		// initializes local table scans
		localTableScan.beforeFirst();
		if(!localTableScan.next()) {
			// create the two local tables with bucketID of 0 and 1
			localTableScan.insert();
			localTableScan.setInt("bucketID", 0);
			localTableScan.setInt("capacity", bucketSize);
			localTableScan.setInt("depth", globalDepth);
			localTableScan.insert();
			localTableScan.setInt("bucketID", 1);
			localTableScan.setInt("capacity", bucketSize);
			localTableScan.setInt("depth", globalDepth);

			// point the global pointers to the local ones
			globalTableScan.insert();
			globalTableScan.setInt("globalBucket", 0);
			globalTableScan.setInt("bucketPointer", 0);
			globalTableScan.setInt("depth", globalDepth);
			globalTableScan.insert();
			globalTableScan.setInt("globalBucket", 1);
			globalTableScan.setInt("bucketPointer", 1);
			globalTableScan.setInt("depth", globalDepth);
		}
		// set the table pointers back to the beginning
		globalTableScan.beforeFirst();
		localTableScan.beforeFirst();
	}

	/**
	 * Positions the index before the first record
	 * having the specified search key.
	 * @param searchkey the search key value.
	 */
	public void beforeFirst(Constant searchkey) {
		close();

		// set the search key for the next method
		this.searchkey = searchkey;

		// find the index
		int index = searchkey.hashCode() % (int)Math.pow(2, globalDepth);

		// open the global scan
		initGlobalTableScan();

		// go to first bucket in global table
		globalTableScan.beforeFirst();
		int pointer = -1;
		while(globalTableScan.next()) {
			int globalID = globalTableScan.getInt("globalBucket");
			if (globalID == index) {
				pointer = globalTableScan.getInt("bucketPointer");
			}
		}

		// bucket not found
		if (pointer == -1) {
			System.out.println("Bucket not found");
		}

		// set current bucket
		currBucket = pointer;

		// set the index before the first record
		String tblName = idxname + currBucket;
		TableInfo ti = new TableInfo(tblName, sch);
		ts = new TableScan(ti, tx);

	}

	/**
	 * Moves the index to the next record having the
	 * search key specified in the beforeFirst method.
	 * Returns false if there are no more such index records.
	 * @return false if no other index records have the search key.
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Returns the dataRID value stored in the current index record.
	 * @return the dataRID stored in the current index record.
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts an index record having the specified
	 * dataval and dataRID values.
	 * @param dataval the dataval in the new index record.
	 * @param datarid the dataRID in the new index record.
	 */
	public void insert(Constant dataval, RID datarid) {
		int counter = 0;
		beforeFirst(dataval);

		// check how many entries are in the table scan
		ts.beforeFirst();
		while(ts.next()) {
			counter += 1;
		}
		ts.beforeFirst();

		// insert into the bucket if the size is correct
		if (counter < bucketSize) {
			ts.insert();
			ts.setInt("block", datarid.blockNumber());
			ts.setInt("id", datarid.id());
			ts.setVal("dataval", dataval);
			return;
		}

		// get the local depth of the bucket
		int depth = -1;
		ts.beforeFirst();
		while(ts.next()) {
			if (localTableScan.getInt("bucketID") == currBucket) {
				depth = localTableScan.getInt("depth");
				break;
			}
		}

		// if the local depth can be increased without increasing the global depth
		if (depth < globalDepth) {
			localTableScan.setInt("depth", depth + 1);

			// split the bucket
			int split = splitBucket(currBucket, depth);
			int newBucketNumber = dataval.hashCode() % (int) Math.pow(2, globalDepth);

			if (newBucketNumber == currBucket && split != 0) {
				ts.beforeFirst();
				ts.insert();
				ts.setInt("block", datarid.blockNumber());
				ts.setInt("id", datarid.id());
				ts.setVal("dataval", dataval);

				localTableScan.beforeFirst();
				boolean found = false;
				while (localTableScan.next()) {
					if (localTableScan.getInt("bucketID") == newBucketNumber) {
						found = true;
						break;
					}
				}

				if (!found) {
					localTableScan.insert();
					localTableScan.setInt("bucketID", newBucketNumber);
					localTableScan.setInt("capacity", bucketSize);
					localTableScan.setInt("depth", globalDepth);

					// update global table
					boolean globalFound = false;
					updateGlobal(newBucketNumber, newBucketNumber);
				}
				return;
			}

			if (newBucketNumber != currBucket && split != bucketSize) {
				String name = idxname + newBucketNumber;
				TableInfo ti = new TableInfo(name, sch);
				TableScan newScan = new TableScan(ti, tx);
				newScan.beforeFirst();
				newScan.insert();
				newScan.setInt("block", datarid.blockNumber());
				newScan.setInt("id", datarid.id());
				newScan.setVal("dataval", dataval);
				newScan.beforeFirst();
				boolean found = false;
				while (newScan.next()) {
					if (newScan.getInt("bucketID") == newBucketNumber) {
						found = true;
						break;
					}
				}
				if (!found) {
					localTableScan.insert();
					localTableScan.setInt("bucketID", newBucketNumber);
					localTableScan.setInt("capacity", bucketSize);
					localTableScan.setInt("depth", globalDepth);
					updateGlobal(newBucketNumber, newBucketNumber);
				}
				return;
			}
		}
		int newMax = ((int)Math.pow(2, (globalDepth+1)))-1;
		globalTableScan.beforeFirst();
		int localDepth;

		for(int i = (int)Math.pow(2, globalDepth); i<newMax+1; i++) {
			if( i > 1) {
				localDepth = i % (int) Math.pow(2, globalDepth);
				String tblname = idxname + localDepth;
				TableInfo ti = new TableInfo(tblname, sch);
				TableScan tsCheckEmpty = new TableScan(ti, tx);
				tsCheckEmpty.beforeFirst();
				boolean empty = true;
				if(tsCheckEmpty.next()) {
					empty = false;
				}
				tsCheckEmpty.close();
				if (!empty && localDepth > 1) {
					globalTableScan.insert();
					globalTableScan.setInt("globalBucket", i);
					globalTableScan.setInt("bucketPointer", localDepth);
					globalTableScan.setInt("depth", globalDepth);
				}

				else if (localDepth < 2) {
					globalTableScan.insert();
					globalTableScan.setInt("globalBucket", i);
					globalTableScan.setInt("bucketPointer", localDepth);
					globalTableScan.setInt("depth", globalDepth);

				} else {
					int x = 1;
					localDepth = i % (int) Math.pow(2, (globalDepth-x));
					while (true) {
						tblname = idxname + localDepth;
						ti = new TableInfo(tblname, sch);
						tsCheckEmpty = new TableScan(ti, tx);
						tsCheckEmpty.beforeFirst();
						empty = true;

						if(tsCheckEmpty.next()) {
							empty = false;
						}
						tsCheckEmpty.close();
						if (!empty && localDepth > 1) {
							globalTableScan.insert();
							globalTableScan.setInt("globalBucket", i);
							globalTableScan.setInt("bucketPointer", localDepth);
							globalTableScan.setInt("depth", globalDepth);
							break;
						}
						else if (localDepth < 2) {
							globalTableScan.insert();
							globalTableScan.setInt("globalBucket", i);
							globalTableScan.setInt("bucketPointer", localDepth);
							globalTableScan.setInt("depth", globalDepth);
							break;
						} else {
							x += 1;
							if(globalDepth - x == 0) {
								break;
							}
							localDepth = i % (int) Math.pow(2, (globalDepth - x));
						}

					}

				}

			}
			// global depth is less than 1
			else {
				System.err.println("Should not be 0 or 1 (globaldepth < 1).");
			}

		}
		// update global depth
		globalDepth += 1;
		globalTableScan.beforeFirst();
		while(globalTableScan.next()) {
			globalTableScan.setInt("depth", globalDepth);
		}

		//split bucket and insert value back into bucket
		int split = splitBucket(currBucket, globalDepth-1);
		int newBucketID = dataval.hashCode() % (int) Math.pow(2, globalDepth);

		//insert into the old bucket
		if (newBucketID == currBucket && split != 0) {
			ts.beforeFirst();
			ts.insert();
			ts.setInt("block", datarid.blockNumber());
			ts.setInt("id", datarid.id());
			ts.setVal("dataval", dataval);

			/*
			 * update the bucket meta data if necessary
			 */

			localTableScan.beforeFirst();
			boolean found = false;
			while (localTableScan.next()) {
				if (localTableScan.getInt("bucketID") == newBucketID) {
					found = true;
					break;
				}
			}

			if (!found) {
				localTableScan.insert();
				localTableScan.setInt("bucketID", newBucketID);
				localTableScan.setInt("capacity", bucketSize);
				localTableScan.setInt("depth", globalDepth);

				//update the global table pointer
				updateGlobal(newBucketID, newBucketID);

			}
			return;
		}
		 // check if we need to insert into a new bucket

		if (newBucketID != currBucket && split != bucketSize) {
			String tblname = idxname + newBucketID;
			TableInfo ti = new TableInfo(tblname, sch);
			TableScan tsNewBucket = new TableScan(ti, tx);

			tsNewBucket.beforeFirst();
			tsNewBucket.insert();
			tsNewBucket.setInt("block", datarid.blockNumber());
			tsNewBucket.setInt("id", datarid.id());
			tsNewBucket.setVal("dataval", dataval);

			/*
			 * update the bucket meta data
			 */

			localTableScan.beforeFirst();
			boolean found = false;
			while (localTableScan.next()) {
				if (localTableScan.getInt("bucketNum") == newBucketID) {
					found = true;
					break;
				}
			}

			if (!found) {
				localTableScan.insert();
				localTableScan.setInt("bucketID", newBucketID);
				localTableScan.setInt("capacity", bucketSize);
				localTableScan.setInt("depth", globalDepth);

				/*
				 * update the global table pointer
				 */
				updateGlobal(newBucketID, newBucketID);

			}
			return;
		}
		insert(dataval, datarid);
		return;
	}

	/**
	 * Deletes the index record having the specified
	 * dataval and dataRID values.
	 * @param dataval the dataval of the deleted index record
	 * @param datarid the dataRID of the deleted index record
	 */
	public void delete(Constant dataval, RID datarid) {
		beforeFirst(dataval);
		while(next())
			if (getDataRid().equals(datarid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index.
	 */
	public void close() {
		closeScan(globalTableScan);
		closeScan(localTableScan);
		if(ts != null) {
			ts.close();
		}
	}

	/**
	 * Initializes the global table scan
	 */
	public void initGlobalTableScan() {
		String tblname = idxname + "GlobalInfo";
		TableInfo ti = new TableInfo(tblname, sch);
		globalTableScan = new TableScan(ti, tx);
		globalTableScan.beforeFirst();
	}

	/**
	 * Initializes the local table scan
	 */
	public void initLocalTableScan() {
		String tblname = idxname + "BucketInfo";
		TableInfo ti = new TableInfo(tblname, sch);
		localTableScan = new TableScan(ti, tx);
		localTableScan.beforeFirst();
	}

	/**
	 * Closes the given table scan
	 */
	public void closeScan(TableScan scan) {
		if(scan != null) {
			scan.close();
		}
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}

	public int splitBucket(int bucketID, int depth) {
		int split = 0;

		// create the new bucket ID
		int newBucketID = 2 * depth + bucketID;

		// create a table scan for the new bucket
		String name = idxname + newBucketID;
		TableInfo ti = new TableInfo(name, sch);
		TableScan newScan = new TableScan(ti, tx);

		newScan.beforeFirst();
		ts.beforeFirst();
		while(ts.next()) {
			Constant val = ts.getVal("dataval");

			if(val.hashCode() % (int)Math.pow(2, depth+1) == newBucketID) {
				newScan.insert();
				newScan.setInt("block", ts.getInt("block"));
				newScan.setInt("id", ts.getInt("id"));
				newScan.setVal("dataval", ts.getVal("dataval"));
				ts.delete();
				split += 1;
			}
		}
		newScan.close();

		if (split!=0) {
			globalTableScan.beforeFirst();
			while(globalTableScan.next()) {
				if(globalTableScan.getInt("globalBucket") == newBucketID) {
					globalTableScan.setInt("bucketPointer", newBucketID);
					localTableScan.beforeFirst();
					while(localTableScan.next()) {
						if(localTableScan.getInt("bucketID") == newBucketID) {
							return -1;
						}
					}
				}
				localTableScan.insert();
				globalTableScan.setInt("bucketID", newBucketID);
				localTableScan.setInt("capacity", bucketSize);
				localTableScan.setInt("depth", globalDepth);
				break;
			}
		}
		return split;
	}

	public boolean updateGlobal(int bucketID, int pointer) {
		globalTableScan.beforeFirst();
		while(globalTableScan.next()) {
			if(globalTableScan.getInt("bucketID") == bucketID) {
				globalTableScan.setInt("bucketPointer", pointer);
				return true;
			}
		}
		return false;
	}

}
