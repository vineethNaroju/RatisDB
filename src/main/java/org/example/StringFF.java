package org.example;

import java.io.*;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;

public class StringFF {

    public static void main(String[] args) throws Exception {

        Map<String, String> map = new TreeMap<>();

        map.put("name", "vineeth");
        map.put("city", "bangalore");


        File file = new File("map");

        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(file.toPath())))) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                out.writeBytes(entry.getKey() + "_" + entry.getValue() + "\n");
            }
        }

        BufferedReader br = new BufferedReader(new FileReader(file));

        String str;

        while((str = br.readLine()) != null) {
            System.out.println(str);
        }


        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(file.toPath())))) {
            System.out.println(in.readUTF());
        }

    }
}
