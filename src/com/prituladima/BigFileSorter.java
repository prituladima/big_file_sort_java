package com.prituladima;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.prituladima.Constants.*;

public class BigFileSorter {
    //Use -Xmx64m to limit Heap size to 64 mb
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        sort();
        long end = System.currentTimeMillis();
        System.out.printf("Sorting time is [%s] ms\n", end - start);
    }

    private static void sort() throws IOException {
        Deque<String> deque = new ArrayDeque<>();
        int index = 0;
        try (Scanner scanner = new Scanner(new File(Constants.BIG_FILE_NAME))) {
            while (scanner.hasNext()) {
                final String name = Util.randomUUID();

                deque.addLast(name);
                System.out.printf("%d. Writing sorted %s file\n", ++index, name);
                File file = new File(TEMP_FILES_FOLDER + name);

                Files.createDirectories(Paths.get(file.getParent()));
                file.createNewFile();


                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);

                for (int j = 0; j < CHUNK_SIZE && scanner.hasNext(); j++) {
                    toBeSorted.add(scanner.next());
                }

                toBeSorted.sort(Comparator.naturalOrder());
                System.out.printf("Count = %d\n", toBeSorted.size());
                try (Writer writer = new BufferedWriter(new FileWriter(file))) {
                    for (String s : toBeSorted) {
                        writer.append(s).append('\n');
                    }
                }
            }
        }

        while (!deque.isEmpty() && deque.size() > 1) {
            String first = deque.removeFirst();
            String second = deque.removeFirst();

            final String name = deque.isEmpty() ? SORTED_FILE_NAME : Util.randomUUID();
            System.out.printf("%d. Merging %s and %s to new created %s file \n", ++index, first, second, name);

            File file = new File(TEMP_FILES_FOLDER + name);

            Files.createDirectories(Paths.get(file.getParent()));
            file.createNewFile();

            try (
                    Scanner scannerFirst = new Scanner(new File(TEMP_FILES_FOLDER + first));
                    Scanner scannerSecond = new Scanner(new File(TEMP_FILES_FOLDER + second));
                    Writer writer = new BufferedWriter(new FileWriter(file));
            ) {
                String valFirst = scannerFirst.hasNext() ? scannerFirst.next() : null;
                String valSecond = scannerSecond.hasNext() ? scannerSecond.next() : null;
                int counter = 0;
                while (valFirst != null || valSecond != null) {
                    if (valSecond == null || (valFirst != null && valFirst.compareTo(valSecond) < 0)) {
                        writer.append(valFirst).append('\n');
                        valFirst = scannerFirst.hasNext() ? scannerFirst.next() : null;

                    } else {
                        writer.append(valSecond).append('\n');
                        valSecond = scannerSecond.hasNext() ? scannerSecond.next() : null;

                    }
                    counter++;
                }

                deque.addLast(name);

                new File(TEMP_FILES_FOLDER + first).delete();
                new File(TEMP_FILES_FOLDER + second).delete();

                System.out.printf("Count = %d\n", counter);
            }

        }
        System.out.println("File was sorted!");

    }

}