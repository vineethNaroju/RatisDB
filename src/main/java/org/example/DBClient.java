package org.example;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBClient implements DBapi, Closeable {
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

    @Override
    public void close() throws IOException {
        raftClient.close();
    }

    public static void main(String[] args) {

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
}
