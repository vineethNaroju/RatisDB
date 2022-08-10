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

        RaftClientReply reply = raftClient.io().send(Message.valueOf("client says hi!"));
        print(reply);
    }
}
