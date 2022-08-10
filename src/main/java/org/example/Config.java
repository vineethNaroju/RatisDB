package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class Config {
     public static final UUID groupUUID;
     public static  List<String> clusterIps;

     static {
         groupUUID = UUID.fromString("e96330a2-c342-4e4d-8799-d1be25537bb1");
         clusterIps = new ArrayList<>();

         clusterIps.add("127.0.0.1:3420");
         clusterIps.add("127.0.0.1:3421");
         clusterIps.add("127.0.0.1:3422");
     }

     private Config() {

     }
}
