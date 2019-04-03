package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.LinkedList;

class BufferTuple {
   int index;
   Buffer buff;

   BufferTuple() {
      index = -1;
      buff = null;
   }

   BufferTuple(int index, Buffer buff) {
      this.index = index;
      this.buff = buff;
   }
}

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
   private HashMap<Integer, Integer> buffMap = new HashMap<>();

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
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
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
      BufferTuple tuple;
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         tuple = chooseUnpinnedBuffer();
         buff = tuple.buff;
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         buffMap.put(blk.blknum, tuple.index);
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
      BufferTuple tuple = chooseUnpinnedBuffer();
      Buffer buff = tuple.buff;
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      buffMap.put(buff.block().blknum, tuple.index);
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
    * Checks if a block exists in a buffer
    * @param blk the block being searched for
    * @return the buffer containing the block, or null if no buffer contains the block
    */
   private Buffer findExistingBuffer(Block blk) {
      if (buffMap.containsKey(blk.blknum)) {
         int num = buffMap.get(blk.blknum);
         return bufferpool[num];
      }
      return null;
   }

   /**
    * Checks for an empty frame, or unpinned frame if none are empty
    * @return the frame for a block to be placed in
    */
   private BufferTuple chooseUnpinnedBuffer() {
      // if there is an empty frame available, return it
      BufferTuple empty = findEmptyBuffer();
      if (empty.buff != null) {
         return empty;
      }

      // if there is no empty frame, then find an unpinned frame to replace
      int i = 0;
      for (Buffer buff : bufferpool) {
         if (!buff.isPinned()) {
            if (buff.block() == null) {
               emptyList.remove(i);
            }
            return new BufferTuple(i, buff);
         }
         i++;
      }
      return new BufferTuple();
   }

   /**
    * Returns an empty buffer in constant time using a linked list
    * @return an empty buffer
    */
   private BufferTuple findEmptyBuffer() {
      if (emptyList.size() > 0) {
         Buffer buff = bufferpool[emptyList.getFirst()];
         emptyList.remove(emptyList.getFirst());
         System.out.println("Found an empty frame!");
         return new BufferTuple(emptyList.getFirst(), bufferpool[emptyList.getFirst()]);
      }
      else return new BufferTuple();
   }
}
