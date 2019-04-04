package simpledb.server;

import simpledb.remote.*;
import java.rmi.registry.*;

public class Test {
    public static void main(String args[]) throws Exception {
        int z; // to keep the repeated code warning away;
        // configure and initialize the database
        SimpleDB.init(args[0]);
        // create a registry specific for the server on the default port
        Registry reg = LocateRegistry.createRegistry(1099);

        // and post the server entry in it
        RemoteDriver d = new RemoteDriverImpl();
        reg.rebind("simpledb", d);

        System.out.println("database server ready");
    }
}

