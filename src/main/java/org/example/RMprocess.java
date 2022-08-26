package org.example;

import org.apache.ratis.protocol.RaftClientRequest;

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

//        if(rmId > 0) {
//            return;
//        }

        Thread input = new Thread(() -> {
            try {
                Thread.sleep(5000);
                simulateRequests(dbServer);
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

    public void simulateRequests(DBapi dBapi) throws Exception {
        String key = "rm" + rmId + "-city", val = "hyd", res;

        for(int i=0; i<20; i++) {
            Thread.sleep(5000L);

            String k = key + (i%5), v = val + (i%5);


            try {
                res = dBapi.query(k);
                print("simulateRequests|query|key:" + k + ",res:" + res);
            } catch (Exception e) {
                print("simulateRequests|query|key:" + k +  ",exception:" + e);
            }

            Thread.sleep(5000L);

            try {
                res = dBapi.update(k, v);
                print("simulateRequests|update|key:" + k + ",res:" + res);
            } catch (Exception e) {
                print("simulateRequests|update|key:" + k + ",exception:" + e);
            }

        }

    }
}
