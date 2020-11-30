package com.prituladima;

import java.io.IOException;

public class Starter {
    public static void main(String[] args) throws IOException {
        BigFileGenerator.main(args);
        BigFileSorter.main(args);
        BigFileSortTester.main(args);
    }
}
