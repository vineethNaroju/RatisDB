package org.example;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/*
*
*
*
*
*
*
*
*
*
* */


public class ServerStateMachine extends BaseStateMachine {

    SimpleStateMachineStorage stateMachineStorage = new SimpleStateMachineStorage();

    Map<String, String> map = new TreeMap<>();

    static class MapState {

        private final TermIndex appliedTermIndex;
        private final Map<String, String> map;


        MapState(TermIndex appliedTermIndex, Map<String, String> map) {
            this.appliedTermIndex = appliedTermIndex;
            this.map = map;
        }

        TermIndex getAppliedTermIndex() {
            return appliedTermIndex;
        }

        Map<String, String> getMap() {
            return map;
        }
    }

    synchronized MapState getMapState() {
        return new MapState(getLastAppliedTermIndex(), map);
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);
        stateMachineStorage.init(raftStorage);
        loadStateMachineFromSnapshot(stateMachineStorage.getLatestSnapshot());
    }


    /*
    * StateMachine -> snap shot -> store in
    *
    *
    *
    * */


    @Override //TODO: takeSnapshot
    public long takeSnapshot() throws IOException {

        // get the map with last applied term index values
        final MapState mapState = getMapState();

        final long appliedTerm = mapState.getAppliedTermIndex().getTerm();
        final long appliedIndex = mapState.getAppliedTermIndex().getIndex();

        // create an empty file with applied term-index values as part of it's name
        final File snapshotFile = stateMachineStorage.getSnapshotFile(appliedTerm, appliedIndex);

        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(snapshotFile.toPath())))) {
            for(Map.Entry<String, String> entry : map.entrySet()) {
                out.writeBytes(entry.getKey() + "_" + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            LOG.warn("failed to write snapshot file \"" + snapshotFile + "\", applied term:" + appliedTerm + " and applied index:" + appliedIndex);
        }

        return super.takeSnapshot();
    }

    void loadStateMachineFromSnapshot(SingleFileSnapshotInfo snapshotInfo) throws IOException {
        if(snapshotInfo == null) {
            LOG.warn("Snapshot info is null");
            return;
        }

        final Path path = snapshotInfo.getFile().getPath();

        if(!Files.exists(path)) {
            LOG.warn("Snapshot file {} doesn't exist for snapshot info {}", path, snapshotInfo);
            return;
        }

        final TermIndex snapshotTermIndex = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());

        File file = path.toFile();

        Stream<String> stringStream = Files.lines(path);

        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(path)))) {

        }
    }

    @Override //TODO: complete query code
    public CompletableFuture<Message> query(Message request) {
        return super.query(request);
    }

    @Override //TODO: complete update code
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        return super.applyTransaction(trx);
    }
}
