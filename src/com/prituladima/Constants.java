package com.prituladima;

public class Constants {
    static final String SORTED_FILE_NAME = "sorted_file.txt";
    static final String BIG_INPUT_FILE = "bigfile_input.txt";
    static final String WORKING_FOLDER = System.getProperty("user.home") + "/IdeaProjects/big_file/";
    static final String BIG_FILE_NAME = WORKING_FOLDER + BIG_INPUT_FILE;
    static final String TEMP_FILES_FOLDER = WORKING_FOLDER + "temp/";

    static final int FILE_SIZE = 1_000_000;
    static final int CHUNK_SIZE = 100_000;
}
