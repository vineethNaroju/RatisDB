package org.example;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;


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

    synchronized boolean updateMapState(TermIndex appliedTermIndex, Map<String, String> newMap) {

        printString("synchronized-updateMapState|appliedTermIndex:" + appliedTermIndex + ",newMap:" + newMap);

        if(updateLastAppliedTermIndex(appliedTermIndex.getTerm(), appliedTermIndex.getIndex())) {
            map = newMap;
            return true;
        }

        return false;
    }

    synchronized boolean updateKeyValue(TermIndex termIndex, String key, String val) {
        if(updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex())) {
            map.put(key, val);
            return true;
        }

        return false;
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        printString("initialize|entered");
        super.initialize(raftServer, raftGroupId, raftStorage);
        stateMachineStorage.init(raftStorage);
        loadStateMachineFromSnapshot(stateMachineStorage.getLatestSnapshot());
        printString("initialize|last line");
    }

    @Override
    public void reinitialize() throws IOException {
        loadStateMachineFromSnapshot(stateMachineStorage.getLatestSnapshot());
    }

    @Override //TODO: takeSnapshot
    public long takeSnapshot() throws IOException {

        printString("takeSnapshot|started");

        // get the map with last applied term index values
        final MapState mapState = getMapState();

        final long appliedTerm = mapState.getAppliedTermIndex().getTerm();
        final long appliedIndex = mapState.getAppliedTermIndex().getIndex();

        printString("takeSnapshot|appliedTerm:" + appliedTerm + ",appliedIndex:" + appliedIndex);

        // create an empty file with applied term-index values as part of it's name
        final File snapshotFile = stateMachineStorage.getSnapshotFile(appliedTerm, appliedIndex);

        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(snapshotFile.toPath()))) {
            for(Map.Entry<String, String> entry : map.entrySet()) {
                String str = entry.getKey() + "_" + entry.getValue();
                printString("takeSnapshot|str:" + str);
                str += "\n";
                outputStream.write(str.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            printString("takeSnapshot|failed to write snapshot file " + snapshotFile + ",term:" + appliedTerm + "," +
                    "index:" + appliedIndex);
            e.printStackTrace();
        }

        printString("takeSnapshot|ended");

        return super.takeSnapshot();
    }

    void loadStateMachineFromSnapshot(SingleFileSnapshotInfo snapshotInfo) throws IOException {
        printString("loadStateMachineFromSnapshot|started");
        if(snapshotInfo == null) {
            printString("loadStateMachineFromSnapshot|Snapshot info is null");
            return;
        }

        final Path path = snapshotInfo.getFile().getPath();

        if(!Files.exists(path)) {
            printString("loadStateMachineFromSnapshot|Snapshot file " + path + " doesn't exist for snapshot info " + snapshotInfo);
            return;
        }

        final TermIndex snapshotTermIndex = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());

        List<String> contents = Files.readAllLines(path);

        final Map<String, String> map = new TreeMap<>();

        for(String line : contents) {
            String[] kv = line.split("_");
            map.put(kv[0], kv[1]);
            printString("loadStateMachineFromSnapshot|snapshotTermIndex:" + snapshotTermIndex + ",key:" + kv[0] + "," +
                    "val:" + kv[1]);
        }

        if(updateMapState(snapshotTermIndex, map)) {
            printString("loadStateMachineFromSnapshot|Loaded state machine from snapshot, term-index: "  + snapshotTermIndex);
        } else {
            printString("loadStateMachineFromSnapshot|Failed to load state machine from snapshot");
        }
    }

    @Override //TODO: complete query code
    public CompletableFuture<Message> query(Message request) {
        String key = request.getContent().toStringUtf8();
        String val = map.getOrDefault(key, "");
        printString("query|key:" + key + ",val:" + val);
        return CompletableFuture.completedFuture(Message.valueOf(val));
    }

    @Override //TODO: complete update code
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        // this the committed entry either from leader or client originated request
        RaftProtos.LogEntryProto entry = trx.getLogEntry();
        String cmd = entry.getStateMachineLogEntry().getLogData().toStringUtf8();

        printString("applyTransaction|node:" + trx.getServerRole().getValueDescriptor().getName() + ",cmd:" + cmd);

        String[] kv = cmd.split("_");
        TermIndex termIndex = TermIndex.valueOf(entry);

        String res = "none";

        if(kv.length >= 2 && updateKeyValue(termIndex, kv[0], kv[1])) {
            printString("applyTransaction|applied trx on local state machine successfully");
            res = "success";
        } else {
            printString("applyTransaction|failed to apply trx on local state machine, kv length: " + kv.length);
            res = "fail";
        }

        return CompletableFuture.completedFuture(Message.valueOf(res));
    }

    void printString(Object o) {
        System.out.println(new Date() + "|" + o);
    }
}
