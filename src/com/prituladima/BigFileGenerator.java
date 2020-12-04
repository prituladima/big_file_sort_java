package com.prituladima;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.prituladima.Constants.CHUNK_SIZE;
import static com.prituladima.Constants.FILE_SIZE;


public class BigFileGenerator {

    public static void main(String[] args) throws IOException {

        File file = new File(Constants.BIG_FILE_NAME);
//        if(file.exists()){
//            return;
//        }
        Files.createDirectories(Paths.get(file.getParent()));
        file.createNewFile();
        long start = System.currentTimeMillis();
        try (Writer myWriter = new BufferedWriter(new FileWriter(file))) {
            for (int i = 0; i < FILE_SIZE; i++) {
                if (i % CHUNK_SIZE == 0) {
                    System.out.printf("Generated %d UUID already \n", i);
                }
                myWriter.append(Util.randomUUID()).append('\n');
            }
        }
        long end = System.currentTimeMillis();
        System.out.printf("File created after: %s ms", end - start);
        //36 -> 1 000 000 -> 1218 ms
        //20 000 -> 555 555 555 -> 676666 ms -> 11 min
    }


}
