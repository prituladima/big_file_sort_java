package com.prituladima;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import static com.prituladima.Constants.*;

public class BigFileSortTester {
    public static void main(String[] args) throws IOException {
        try (BufferedReader scanner = new BufferedReader(new FileReader(FileUtil.file(TEMP_FILES_FOLDER + "7a6a51ea-2b0d-4662-bc11-1f6071758d21")))) {
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