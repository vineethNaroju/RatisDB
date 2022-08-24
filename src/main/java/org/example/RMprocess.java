package org.example;

import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class RMprocess {

    private final DBServer dbServer;
    private final DBClient dbClient;

    private final int rmId;

    final CountDownLatch latch = new CountDownLatch(1);

    RMprocess(int id) throws Exception {
        rmId = id;
        dbServer = new DBServer(id);
        dbServer.raftServer.start();

        dbClient = new DBClient();
    }

    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public void loopOver() {

        if(rmId > 0) {
            return;
        }

        Thread input = new Thread(() -> {
            try {
                dbClient.doStuff();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        });

        input.start();
    }

    public static void main(String[] args) throws Exception {

        RMprocess rm = new RMprocess(Integer.parseInt(args[0]));

        rm.loopOver();

        try {
            rm.latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
