package com.prituladima;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import static com.prituladima.Constants.*;

public class BigFileSortTester {
    public static void main(String[] args) throws IOException {
        try (Scanner scanner = new Scanner(new File(TEMP_FILES_FOLDER + SORTED_FILE_NAME))) {
            String first = scanner.nextLine();
            int count = 1;
            while (scanner.hasNextLine()) {
                String second = scanner.nextLine();
                if (first.compareTo(second) > 0) {
                    throw new RuntimeException("Your algo is wrong!");
                }
                first = second;
                count++;
            }
            if (count != 1_000_000) {
                throw new RuntimeException("Your algo is wrong! " + count);
            }
        }
    }
}