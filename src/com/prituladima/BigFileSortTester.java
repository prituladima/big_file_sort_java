package com.prituladima;

import java.io.IOException;
import java.util.Scanner;

import static com.prituladima.Constants.*;

public class BigFileSortTester {
    public static void main(String[] args) throws IOException {
        try (BufferedScanner scanner = new BufferedScanner(FileUtil.file(TEMP_FILES_FOLDER + "861610e4-8f6a-4b58-a58d-720ec60a1be9"))) {
            String prev = scanner.next();
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