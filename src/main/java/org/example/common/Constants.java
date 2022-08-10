/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.common;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * Constants across servers and clients
 */
public final class Constants {
    public static final List<RaftPeer> PEERS;

    static {
        final Properties properties = new Properties();

        final String[] addresses = {"127.0.0.1:3420", "127.0.0.1:3421", "127.0.0.1:3422"};

        final List<RaftPeer> peers = new ArrayList<>(addresses.length);

        for (int i = 0; i < addresses.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(addresses[i]).build());
        }
        PEERS = Collections.unmodifiableList(peers);
    }

    private static final UUID GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(RaftGroupId.valueOf(Constants.GROUP_ID), PEERS);

    private Constants() {
    }
}