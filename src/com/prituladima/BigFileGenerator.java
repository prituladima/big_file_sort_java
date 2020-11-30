package com.prituladima;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;


public class BigFileGenerator {

    public static void main(String[] args) throws IOException {

        File file = new File(Constants.BIG_FILE_NAME);
//        if(file.exists()){
//            return;
//        }
        Files.createDirectories(Paths.get(file.getParent()));
        file.createNewFile();
        long start = System.currentTimeMillis();
        Writer myWriter = new BufferedWriter(new FileWriter(file));
        for (int i = 0 ; i < 1_000_000; i++) {
            myWriter.write(Util.randomUUID());
            myWriter.write('\n');
        }
        myWriter.close();
        long end = System.currentTimeMillis();
        System.out.printf("File created after: %s ms", end - start);
        //36 -> 1 000 000 -> 1218 ms
        //20 000 -> 555 555 555 -> 676666 ms -> 11 min
    }


}
