package org.example;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.api.*;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBClient {


    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public static void main(String[] args) throws Exception {
        DBClient client = new DBClient();
        client.doStuff();
    }

    private void doStuff() throws Exception {

        RaftProperties raftProperties = new RaftProperties();

        List<RaftPeer> raftPeersList = new ArrayList<>();

        for(String peerIp : Config.clusterIps) {
            raftPeersList.add(RaftPeer.newBuilder().setId(peerIp)
                    .setAddress(peerIp).build());
        }

        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(Config.groupUUID), raftPeersList);

        RaftClient raftClient = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(raftGroup)
                .build();

        RaftClientReply reply = raftClient.io().send(Message.valueOf("send message"));
        // goes to applyTransaction
        print(reply.getMessage());

        reply = raftClient.io().sendReadOnly(Message.valueOf("read-only message"));
        // goes to query
        print(reply.getMessage());

        String[] updates = {"abc_def", "abc_1", "cat_kitten", "cat_cute", "hello_world", "prime_sun", "cat", "hello",
                "abc", "who", "rose"};


        for(String entry : updates) {
            String[] kv = entry.split("_");

            if(kv.length == 1) {
                reply = raftClient.io().sendReadOnly(Message.valueOf(entry));
            } else {
                reply = raftClient.io().send(Message.valueOf(entry));
            }

            print("input:" + entry + ",reply message:" + reply.getMessage().getContent().toStringUtf8());
        }
    }
}
