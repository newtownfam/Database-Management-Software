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
			localTableScan.setInt("capacity", globalDepth);
			localTableScan.insert();
			localTableScan.setInt("bucketID", 1);
			localTableScan.setInt("capacity", bucketSize);
			localTableScan.setInt("capacity", globalDepth);

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

}
