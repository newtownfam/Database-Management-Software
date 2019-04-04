package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private LinkedList<Integer> emptyList = new LinkedList<>();

   // <block id, buffer index>
   private HashMap<Block, Integer> buffMap = new HashMap<>();

   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      int x = -1;
      for (int i=0; i<numbuffs; i++) {

      	// create a new buffer and set the index so it can be easily found later
      	Buffer buff = new Buffer();
      	buff.setIndex(i);

      	// add the buffer to the buffer pool
      	bufferpool[i] = buff;

      	// since the pool buffers are empty have the empty list point to all buffers
      	emptyList.add(i);
      }
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }

   /**
    * Pins a buffer to the specified block.
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         int index = buff.getIndex();
         buff.assignToBlock(blk);

         // add an entry to the buffer in the hashmap
         buffMap.put(blk, index);

         // set the current time for the LRU policy
         buff.setTime(System.currentTimeMillis());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }

   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it.
    * Returns null (without allocating the block) if
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      int index = buff.getIndex();
      buff.assignToNew(filename, fmtr);

      // add an entry to the buffer in the hashmap
      buffMap.put(buff.block(), index);

      // set the current time for the LRU policy
      buff.setTime(System.currentTimeMillis());

      numAvailable--;
      buff.pin();
      return buff;
   }

   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }

   /**
    * Checks if a block exists in a buffer, return it using a hashmap in constant time
    * @param blk the block being searched for
    * @return the buffer containing the block, or null if no buffer contains the block
    */
   private Buffer findExistingBuffer(Block blk) {
      // check the hashmap if the block exists in the buffer pool
	   if(buffMap.containsKey(blk)) {
	   	 return bufferpool[buffMap.get(blk)];
	   }
	   return null;
   }

   /**
    * Checks for an empty frame, or unpinned frame if none are empty
    * @return the frame for a block to be placed in
    */
   private Buffer chooseUnpinnedBuffer() {
      // if there is an empty frame available, return it
      Buffer empty = findEmptyBuffer();
      if (empty != null) {
         return empty;
      }

      Buffer lruBuff = leastRecentlyUsed();
      buffMap.remove(lruBuff.block());
      return lruBuff;
   }

   /**
    * Returns an empty buffer in constant time using a linked list
    * @return an empty buffer
    */
   private Buffer findEmptyBuffer() {
      if (emptyList.size() > 0) {
         Buffer buff = bufferpool[emptyList.getFirst()];
         emptyList.remove(emptyList.getFirst());
         System.out.println("Found an empty frame!");
         return buff;
      }
      else return null;
   }

    /**
     * function to calculate least recently used buffer in pool
     * @return lru buffer
     */
   private Buffer leastRecentlyUsed() {
      long lruTime = bufferpool[0].getTime();
      Buffer lruBuff = bufferpool[0];
      for (Buffer buff: bufferpool) {
         if (buff.getTime() < lruTime && !buff.isPinned()) {
            lruTime = buff.getTime();
            lruBuff = buff;
         }
      }
      return lruBuff;
   }

    /**
     * getter for bufferpool in BufferMgr class
     ** @return
     */
    Buffer[] getBufferpool() {
        return bufferpool;
    }

    /**
     * Override of toString method to return info on all buffers in bufferpool concatenated into one string
     * @return string concatenation
     */
   @Override
   public String toString() {
       String ans = "";
       int i = 0;
      for (Buffer buff:bufferpool) {
          ans = ans.concat("Item " + i + " in Bufferpool: \n" + buff.toString() + "\n");
          i++;
      }
      return ans;
   }
}
