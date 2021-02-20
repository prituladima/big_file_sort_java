package com.prituladima;

public class Constants {
    static final String BIG_INPUT_FILE = "bigfile_input.txt";
    static final String WORKING_FOLDER = System.getProperty("user.home") + "/IdeaProjects/big_file/";
    static final String TEMP_FILES_FOLDER = "temp/";

    static final int FILE_SIZE = 555_555_555/(20*5);
    static final int CHUNK_SIZE = 8_000;

    static boolean nativeCodeAllowed = false;
    static boolean debug = false;

}
