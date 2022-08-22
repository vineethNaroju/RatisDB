package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class StringFF {

    public static void main(String[] args) throws Exception {

        Map<String, String> map = new TreeMap<>();

        map.put("name", "vineeth");
        map.put("city", "bangalore");

        File file = new File("kvpair.txt");

        BufferedWriter bf = null;

        try {

            bf = new BufferedWriter(new FileWriter(file));

            for (Map.Entry<String, String> entry : map.entrySet()) {
                bf.write(entry.getKey() + "_" + entry.getValue() + "\n");
            }

            bf.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert bf != null;
                bf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Path path = file.toPath();

        List<String> contents = Files.readAllLines(path);
        for(String line : contents) {
            String[] kv = line.split("_");
            System.out.println(kv[0] + "|" + kv[1]);
        }
    }
}
