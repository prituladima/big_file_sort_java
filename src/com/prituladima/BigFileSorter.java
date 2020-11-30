package com.prituladima;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.prituladima.Constants.*;

public class BigFileSorter {
    //Use -Xmx64m to limit Heap size to 64 mb
    public static void main(String[] args) throws IOException {


        //naiveSort();
        parrSort();


    }

    private static void parrSort() throws IOException {
        final List<String> files = new ArrayList<>();
        int index = 0;
        try (Scanner scanner = new Scanner(new File(Constants.BIG_FILE_NAME))) {
            while (scanner.hasNextLine()) {
                final String name = Util.randomUUID();

                files.add(name);
                System.out.printf("%d. Writing sorted %s file\n", ++index, name);
                File file = new File(TEMP_FILES_FOLDER + name);

                Files.createDirectories(Paths.get(file.getParent()));
                file.createNewFile();

                List<String> toBeSorted = new ArrayList<>(100_000);
                int size = 100_000;
                while (size-- > 0 & scanner.hasNextLine()) {
                    toBeSorted.add(scanner.nextLine());
                }

                toBeSorted.sort(Comparator.naturalOrder());

                final Writer writer = new BufferedWriter(new FileWriter(file));

                for (String s : toBeSorted) {
                    writer.append(s).append('\n');
                }
                writer.close();
            }
        }
        Deque<String> deque = new ArrayDeque<>(files);
        while (!deque.isEmpty() && deque.size() > 1) {
            String first = deque.removeFirst();
            String second = deque.removeFirst();

            final String name = deque.isEmpty() ?  SORTED_FILE_NAME : Util.randomUUID();
            System.out.printf("%d. Merging %s and %s to new created %s file \n", ++index, first, second, name);

            File file = new File(TEMP_FILES_FOLDER + name);

            Files.createDirectories(Paths.get(file.getParent()));
            file.createNewFile();

            try (
                    Scanner scannerFirst = new Scanner(new File(TEMP_FILES_FOLDER + first));
                    Scanner scannerSecond = new Scanner(new File(TEMP_FILES_FOLDER + second));
                    Writer writer = new BufferedWriter(new FileWriter(file));
            ) {
                String valFirst = scannerFirst.nextLine();
                String valSecond = scannerSecond.nextLine();

                while (scannerFirst.hasNextLine() && scannerSecond.hasNextLine()) {
                    if (valFirst.compareTo(valSecond) < 0) {
                        writer.append(valFirst).append('\n');
                        valFirst = scannerFirst.nextLine();
                    } else {
                        writer.append(valSecond).append('\n');
                        valSecond = scannerSecond.nextLine();
                    }
                }

                while (scannerFirst.hasNextLine()) {
                    writer.append(valFirst = scannerFirst.nextLine()).append('\n');
                }

                while (scannerFirst.hasNextLine()) {
                    writer.append(valSecond = scannerSecond.nextLine()).append('\n');
                }

                deque.add(name);
                new File(TEMP_FILES_FOLDER + first).delete();
                new File(TEMP_FILES_FOLDER + second).delete();
            }

        }
        System.out.println("File was sorted!");

    }


    private static void naiveSort() throws IOException {
        List<String> list = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(Constants.BIG_FILE_NAME))) {
            int i = 0;
            while (scanner.hasNextLine()) {
                list.add(scanner.nextLine());
                i++;
                if (i % 1000 == 0) {
                    System.out.println(i);
                }
            }
        }

        list.sort(Comparator.naturalOrder());

//        try (Writer writer = new BufferedWriter(new FileWriter(TEMP_FILES_FOLDER + SORTED_FILE_NAME))) {
//            Files.createDirectories(Paths.get(file.getParent()));
//            file.createNewFile();
//        }
    }


}
