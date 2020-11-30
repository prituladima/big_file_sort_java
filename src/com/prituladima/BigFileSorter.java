package com.prituladima;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import static com.prituladima.Constants.*;

public class BigFileSorter {
    //Use -Xmx64m to limit Heap size to 64 mb
    public static void main(String[] args) throws IOException {

        long start = System.currentTimeMillis();
        //naiveSort();
        parrSort();
        long end = System.currentTimeMillis();
        System.out.printf("Time of sort is : %s ms\n", end - start);
    }

    private static void parrSort() throws IOException {
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

        System.out.println("File was sorted!");

        System.out.println("Name: " + ForkJoinPool.commonPool().invoke(new MergeSort<String>(new ArrayList<>(deque))));

    }

    public static class MergeSort<N extends Comparable<N>> extends RecursiveTask<String> {
        private List<String> fileNames;

        public MergeSort(List<String> elements) {
            this.fileNames = new ArrayList<>(elements);
        }

        @Override
        protected String compute() {
            if (this.fileNames.size() <= 1)
                return this.fileNames.get(0);
            else {
                final int pivot = this.fileNames.size() / 2;
                MergeSort<N> leftTask = new MergeSort<N>(this.fileNames.subList(0, pivot));
                MergeSort<N> rightTask = new MergeSort<N>(this.fileNames.subList(pivot, this.fileNames.size()));

                leftTask.fork();
                rightTask.fork();

                String left = leftTask.join();
                String right = rightTask.join();

                try {
                    return merge(left, right);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }

        private String merge(String first, String second) throws IOException {
            String res = Util.randomUUID();
            System.out.printf(" Merging %s and %s to new created %s file \n", first, second, res);

            File file = new File(TEMP_FILES_FOLDER + res);


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

                new File(TEMP_FILES_FOLDER + first).delete();
                new File(TEMP_FILES_FOLDER + second).delete();

                System.out.printf("Count = %d\n", counter);
            }


            return res;
        }
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
