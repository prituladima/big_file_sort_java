package com.prituladima;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.prituladima.Constants.*;

public class BigFileSortTester {
    public static void main(String[] args) throws IOException {
        final File[] directoryListing = FileUtil.file(TEMP_FILES_FOLDER).listFiles();
        if (directoryListing != null && directoryListing.length > 0) {
            for (File outputFileToCheck : directoryListing) {
                if (outputFileToCheck.isFile()) {
                    try (BufferedReader scanner = new BufferedReader(new FileReader(outputFileToCheck))) {
                        String prev = scanner.readLine();
                        String cur;
                        int count = 1;
                        while ((cur = scanner.readLine()) != null) {
                            if (prev.compareTo(cur) > 0) {
                                throw new RuntimeException("Your algo is wrong!");
                            }
                            prev = cur;
                            count++;
                        }

                        if (count != FILE_SIZE) {
                            throw new RuntimeException("Your algo is wrong! " + count);
                        }
                        System.out.println("File is sorted correctly!");
                    }

                }
            }
        } else {
            throw new RuntimeException("No output found!");
        }
    }
}