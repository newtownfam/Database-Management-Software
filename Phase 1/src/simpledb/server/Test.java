package simpledb.server;
import simpledb.buffer.*;
import simpledb.file.Block;

public class Test {
    public static void main(String args[]) throws Exception {
        for (int i = 10; i < Math.pow(2, 16); i += 15000) {
            BufferMgr manager = new BufferMgr(i);
            AdvBufferMgr advManager = new AdvBufferMgr(i);

            System.out.println("********************* " + i + " buffers *********************");
            System.out.println("------Filling buffers------\n");
            System.out.println("ADVANCED MANAGER: ");
            long start = System.currentTimeMillis();
            for (int j = 1; j < i-1; j++) {
                Block blk = new Block("Fakefile.txt", j);
                advManager.pin(blk);
            }
            long end = System.currentTimeMillis();
            long finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("BASIC MANAGER: ");
            start = System.currentTimeMillis();
            for (int j = 1; j < i-1; j++) {
                Block blk = new Block("Fakefile.txt", j);
                manager.pin(blk);
            }
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("------Finding last empty buffer------\n");
            Block blk = new Block("LastFake.txt", 99999999);

            System.out.println("ADVANCED MANAGER: ");
            start = System.currentTimeMillis();
            advManager.pin(blk);
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("BASIC MANAGER: ");
            start = System.currentTimeMillis();
            manager.pin(blk);
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("------Searching for a specific block (located at the end of the bufferpool)------\n");

            System.out.println("ADVANCED MANAGER: ");
            start = System.currentTimeMillis();
            advManager.pin(blk);
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("BASIC MANAGER: ");
            start = System.currentTimeMillis();
            manager.pin(blk);
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            int r = i/10;
            System.out.println("------Replacing " + r + " blocks------\n");
            System.out.println("ADVANCED MANAGER: ");
            start = System.currentTimeMillis();
            for (int replace = 0; replace < r; replace += 1) {
                Block blk2 = new Block("ReplaceFile.txt", r);
                    advManager.pin(blk2);
            }
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");

            System.out.println("BASIC MANAGER: ");
            start = System.currentTimeMillis();
            for (int replace = 0; replace < r; replace += 1) {
                Block blk2 = new Block("ReplaceFile.txt", r);
                manager.pin(blk2);
            }
            end = System.currentTimeMillis();
            finish = end - start;
            System.out.println("Finished in: " + finish + " ms\n");


            System.out.println("*********************************************************");
        }
    }
}

