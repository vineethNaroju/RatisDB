package org.example;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.NetUtils;
import sun.misc.UUEncoder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DBServer implements Closeable, DBapi {

    public final RaftServer raftServer;
    public final int peerIdx;

    public final ClientId clientId;

    public final RaftGroup raftGroup;

    DBServer(int peerIdx) throws Exception {
        this.peerIdx = peerIdx;

        String temp = "DBServer-" + peerIdx;

        clientId = ClientId.valueOf(UUID.nameUUIDFromBytes(temp.getBytes()));

        RaftProperties raftProperties = new RaftProperties();

        List<RaftPeer> raftPeersList = new ArrayList<>();

        for(String peerIp : Config.clusterIps) {
            raftPeersList.add(RaftPeer.newBuilder().setId(peerIp)
                    .setAddress(peerIp).build());
        }

        raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(Config.groupUUID), raftPeersList);

        RaftPeer currentRaftPeer = raftPeersList.get(peerIdx);

        File storageDir = new File("./" + currentRaftPeer.getId());

        RaftServerConfigKeys.setStorageDir(raftProperties, Collections.singletonList(storageDir));

        // what does this mean ?
        final int serverPort = NetUtils.createSocketAddr(currentRaftPeer.getAddress()).getPort();

        GrpcConfigKeys.Server.setPort(raftProperties, serverPort);
//        NettyConfigKeys.Server.setPort(raftProperties, serverPort);

        final ServerStateMachine stateMachine = new ServerStateMachine();

//        raftProperties.setBoolean(RaftServerConfigKeys.Snapshot.AUTO_TRIGGER_ENABLED_KEY, true);
//        raftProperties.setInt(RaftServerConfigKeys.Snapshot.AUTO_TRIGGER_THRESHOLD_KEY, 10);
//        raftProperties.setInt(RaftServerConfigKeys.Snapshot.CREATION_GAP_KEY, 5);
//        raftProperties.setInt(RaftServerConfigKeys.Snapshot.RETENTION_FILE_NUM_KEY, 2);

        //RaftServerConfigKeys.java

        raftServer = RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(raftProperties)
                .setServerId(currentRaftPeer.getId())
                .setStateMachine(stateMachine)
                .build();
    }

    @Override
    public void close() throws IOException {
        raftServer.close();
    }

    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public static void main(String[] args) throws Exception {
        try (DBServer server = new DBServer(Integer.parseInt(args[0]))) {
            server.raftServer.start();
            // otherwise server exits immediately - wtf
            new Scanner(System.in, UTF_8.name()).nextLine();
        }
    }

    public String update(String key, String val) throws IOException {
        RaftClientRequest req = RaftClientRequest.newBuilder()
                .setMessage(Message.valueOf(key + "_" + val))
                .setType(RaftClientRequest.writeRequestType())
                .setGroupId(raftGroup.getGroupId())
                .setServerId(raftServer.getId())
                .setClientId(clientId)
                //.setLeaderId(raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLeaderId())
                .build();
        RaftClientReply reply = raftServer.submitClientRequest(req);
        handleReply(reply);
        return reply.getMessage().getContent().toStringUtf8();
    }

    public String query(String key) throws IOException {
        RaftClientRequest req = RaftClientRequest.newBuilder()
                .setMessage(Message.valueOf(key))
                .setType(RaftClientRequest.readRequestType())
                .setGroupId(raftGroup.getGroupId())
                .setServerId(raftServer.getId())
                .setClientId(clientId)
                //.setLeaderId(raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLeaderId())
                .build();
        RaftClientReply reply = raftServer.submitClientRequest(req);
        handleReply(reply);
        return reply.getMessage().getContent().toStringUtf8();
    }

    public void handleReply(RaftClientReply reply) {
        if(reply.isSuccess()) {
            return;
        }

        NotLeaderException notLeaderException = reply.getNotLeaderException();

        if(notLeaderException != null) {
            print(notLeaderException);
            return;
        }

        LeaderNotReadyException leaderNotReadyException = reply.getLeaderNotReadyException();

        if(leaderNotReadyException != null) {
            print(leaderNotReadyException);
            return;
        }

        StateMachineException stateMachineException = reply.getStateMachineException();

        if(stateMachineException != null) {
            print(stateMachineException);
            return;
        }

        RaftException raftException =  reply.getException();

        if(raftException != null) {
            print(raftException);
            return;
        }

        print(reply);

    }
}

/*
* Fri Aug 26 01:13:25 IST 2022|RaftClientReply:client-3CDAA9A573B6->127.0.0.1:3421@group-D1BE25537BB1, cid=0, FAILED org.apache.ratis.protocol.exceptions.NotLeaderException: Server 127.0.0.1:3421@group-D1BE25537BB1 is not the leader, logIndex=0, commits[127.0.0.1:3421:c-1]

 * * */
