package com.prituladima;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class FileUtil {

    private static String defaultFolderPath;

    static {
        if (!Constants.WORKING_FOLDER.endsWith("/")) {
            throw new RuntimeException("WORKING_FOLDER must end with /");
        }
        setDefaultFolder(Constants.WORKING_FOLDER);
    }

    public static void setDefaultFolder(String defaultFolderPath) {
        FileUtil.defaultFolderPath = defaultFolderPath;
    }

    public static File file(String fileName) {
        return new File(defaultFolderPath + fileName);
    }

    public static File createFile(String fileName) {
        try {
            final File file = new File(defaultFolderPath + fileName);
            Files.createDirectories(Paths.get(file.getParent()));
            file.createNewFile();
            return file;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean removeFile(String fileName) {
        return new File(defaultFolderPath + fileName).delete();
    }


}
