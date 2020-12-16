package com.prituladima;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static com.prituladima.Constants.*;

public class BigFileSorter {
    //Use -Xmx64m to limit Heap size to 64 mb
    public static void main(String[] args) throws IOException {
        long sortingTime;
        long parallelSortingTime;
        {
            long start = System.currentTimeMillis();
            sort();
            long end = System.currentTimeMillis();
            sortingTime = end - start;
        }
        {
            long start = System.currentTimeMillis();
            parallelSort();
            long end = System.currentTimeMillis();
            parallelSortingTime = end - start;
        }
        System.out.printf("File size is %d strings\n", FILE_SIZE);
        System.out.printf("Sorting time is [%s] ms\n", sortingTime);
        System.out.printf("Parallel Sorting time is [%s] ms\n", parallelSortingTime);
        System.out.printf("Better [%f] %%\n", ((sortingTime - parallelSortingTime) * 1.0 / sortingTime) * 100);
    }

    private static void sort() throws IOException {
        Deque<String> deque = new ArrayDeque<>();
        int index = 0;
        try (BufferedScanner scanner = new BufferedScanner(FileUtil.file(Constants.BIG_INPUT_FILE))) {
            while (scanner.hasNext()) {
                final String name = Util.randomUUID();

                deque.addLast(name);
                System.out.printf("%d. Writing sorted %s file\n", ++index, name);

                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);

                for (int j = 0; j < CHUNK_SIZE && scanner.hasNext(); j++) {
                    toBeSorted.add(scanner.next());
                }

                toBeSorted.sort(Comparator.naturalOrder());
                System.out.printf("Count = %d\n", toBeSorted.size());
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    for (String s : toBeSorted) {
                        writer.append(s).append('\n');
                    }
                }
            }
        }

        while (deque.size() > 1) {
            String first = deque.removeFirst();
            String second = deque.removeFirst();

            String res = Util.randomUUID();

            merge(TEMP_FILES_FOLDER + first, TEMP_FILES_FOLDER + second, TEMP_FILES_FOLDER + res);
            deque.addLast(res);
        }
        System.out.println("File was sorted!");

    }


    private static void parallelSort() throws IOException {
        List<String> list = new ArrayList<>();
        int index = 0;
        try (BufferedScanner scanner = new BufferedScanner(FileUtil.file(Constants.BIG_INPUT_FILE))) {
            while (scanner.hasNext()) {
                final String name = Util.randomUUID();

                list.add(name);
                System.out.printf("%d. Writing sorted %s file\n", index++, name);

                int count = 0;
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    for (int j = 0; j < CHUNK_SIZE && scanner.hasNext(); j++, count++) {
                        writer.append(scanner.next()).append('\n');
                    }
                } catch (IOException ioException) {
                    //ignore
                }
                System.out.printf("Count = %d\n", count);
            }
        }

        System.out.println("Name: " + ForkJoinPool.commonPool().invoke(new MergeSort<String>(list)));
        System.out.println("File was sorted!");
    }

    public static final class MergeSort<N extends Comparable<N>> extends RecursiveTask<String> {
        private List<String> fileNames;

        public MergeSort(List<String> elements) {
            this.fileNames = new ArrayList<>(elements);
        }

        @Override
        protected String compute() {
            if (this.fileNames.size() <= 1) {
                final List<String> toBeSorted = new ArrayList<>();
                final String fileName = TEMP_FILES_FOLDER + this.fileNames.get(0);
                try (BufferedScanner scanner = new BufferedScanner(FileUtil.file(fileName))) {
                    while (scanner.hasNext()) {
                        toBeSorted.add(scanner.next());
                    }
                } catch (IOException ioException) {
                    //ignore
                }
                toBeSorted.sort(Comparator.naturalOrder());
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.file(fileName)))) {
                    for (String s : toBeSorted) {
                        writer.append(s).append('\n');
                    }
                } catch (IOException ioException) {
                    //ignore
                }
                return this.fileNames.get(0);
            } else {
                final int pivot = this.fileNames.size() / 2;
                MergeSort<N> leftTask = new MergeSort<>(this.fileNames.subList(0, pivot));
                MergeSort<N> rightTask = new MergeSort<>(this.fileNames.subList(pivot, this.fileNames.size()));

                leftTask.fork();
                rightTask.fork();

                String left = leftTask.join();
                String right = rightTask.join();

                try {
                    String res = Util.randomUUID();
                    merge(TEMP_FILES_FOLDER + left, TEMP_FILES_FOLDER + right, TEMP_FILES_FOLDER + res);
                    return res;
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
    }

    private static void merge(String first, String second, String res) throws IOException {
        System.out.printf(" Merging %s and %s to new created %s file \n", first, second, res);

        try (
                BufferedScanner scannerFirst = new BufferedScanner(FileUtil.file(first));
                BufferedScanner scannerSecond = new BufferedScanner(FileUtil.file(second));
                Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(res)));
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

            FileUtil.removeFile(first);
            FileUtil.removeFile(second);

            System.out.printf("Count = %d\n", counter);
        }
    }
}