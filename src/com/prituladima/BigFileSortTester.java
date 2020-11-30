package com.prituladima;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import static com.prituladima.Constants.*;

public class BigFileSortTester {
    public static void main(String[] args) throws IOException {
        try (Scanner scanner = new Scanner(new File(TEMP_FILES_FOLDER + SORTED_FILE_NAME))) {
            String prev = scanner.nextLine();
            int count = 1;
            while (scanner.hasNext()) {
                String cur = scanner.next();
                if (prev.compareTo(cur) > 0) {
                    throw new RuntimeException("Your algo is wrong!");
                }
                prev = cur;
                count++;
            }

            if (count != FILE_SIZE) {
                throw new RuntimeException("Your algo is wrong! " + count);
            }
        }
    }
}