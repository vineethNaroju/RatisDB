package org.example;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DBServer implements Closeable, DBapi {

    public final RaftServer raftServer;
    public final int peerIdx;

    DBServer(int peerIdx) throws Exception {
        this.peerIdx = peerIdx;

        RaftProperties raftProperties = new RaftProperties();

        List<RaftPeer> raftPeersList = new ArrayList<>();

        for(String peerIp : Config.clusterIps) {
            raftPeersList.add(RaftPeer.newBuilder().setId(peerIp)
                    .setAddress(peerIp).build());
        }

        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(Config.groupUUID), raftPeersList);

        RaftPeer currentRaftPeer = raftPeersList.get(peerIdx);

        File storageDir = new File("./" + currentRaftPeer.getId());

        RaftServerConfigKeys.setStorageDir(raftProperties, Collections.singletonList(storageDir));

        // what does this mean ?
        final int serverPort = NetUtils.createSocketAddr(currentRaftPeer.getAddress()).getPort();

        GrpcConfigKeys.Server.setPort(raftProperties, serverPort);
//        NettyConfigKeys.Server.setPort(raftProperties, serverPort);

        final ServerStateMachine stateMachine = new ServerStateMachine();

        raftProperties.setBoolean(RaftServerConfigKeys.Snapshot.AUTO_TRIGGER_ENABLED_KEY, true);
        raftProperties.setInt(RaftServerConfigKeys.Snapshot.AUTO_TRIGGER_THRESHOLD_KEY, 10);
        raftProperties.setInt(RaftServerConfigKeys.Snapshot.CREATION_GAP_KEY, 5);
        raftProperties.setInt(RaftServerConfigKeys.Snapshot.RETENTION_FILE_NUM_KEY, 2);

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
                .build();
        RaftClientReply reply = raftServer.submitClientRequest(req);
        return reply.getMessage().getContent().toStringUtf8();
    }

    public String query(String key) throws IOException {
        RaftClientRequest req = RaftClientRequest.newBuilder()
                .setMessage(Message.valueOf(key))
                .setType(RaftClientRequest.readRequestType())
                .build();
        RaftClientReply reply = raftServer.submitClientRequest(req);
        return reply.getMessage().getContent().toStringUtf8();
    }
}
