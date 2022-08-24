package org.example;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBClient {
    public final RaftClient raftClient;
    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    DBClient() {
        List<RaftPeer> raftPeersList = new ArrayList<>();

        for(String peerIp : Config.clusterIps) {
            raftPeersList.add(RaftPeer.newBuilder().setId(peerIp)
                    .setAddress(peerIp).build());
        }

        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(Config.groupUUID), raftPeersList);

        raftClient = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(raftGroup)
                .build();
    }

    public static void main(String[] args) throws Exception {
        DBClient client = new DBClient();
        client.doStuff();
    }

    // goes to query
    public String query(String key) throws IOException {
        RaftClientReply reply = raftClient.io().sendReadOnly(Message.valueOf(key));
        return reply.getMessage().getContent().toStringUtf8();
    }

    // goes to applyTransaction
    public String update(String key, String val) throws IOException {
        RaftClientReply reply = raftClient.io().send(Message.valueOf(key + "_" + val));
        return reply.getMessage().getContent().toStringUtf8();
    }

    public void doStuff() throws Exception {
        String key = "city", val = "hyd", res = "none";

        for(int i=0; i<100; i++) {
            Thread.sleep(3000);

            res = query(key);

            print("query|key:" + key + ",val:" + val);

            Thread.sleep(2000);

            res = update(key, val + i);

            print("update|key:" + key + ",val" + val + i);
        }
    }
}
