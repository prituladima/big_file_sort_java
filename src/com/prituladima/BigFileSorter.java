package com.prituladima;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.prituladima.Constants.*;

public class BigFileSorter {

    public final static int availableProcessors = Runtime.getRuntime().availableProcessors();
    public final static AtomicInteger usedProcessors = new AtomicInteger(0);

    //Use -Xmx64m to limit Heap size to 64 mb
    //-XX:ActiveProcessorCount=16
    public static void main(String[] args) throws IOException {
        int tests = 1;
        for (int i = 0; i < tests; i++) {
            long sortingTime;
            long parallelSortingTime;
            long naiveParallelSortingTime;
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
            {
                long start = System.currentTimeMillis();
                naiveParallelSort();
                long end = System.currentTimeMillis();
                naiveParallelSortingTime = end - start;
            }
//        System.out.printf("File size is %d strings\n", FILE_SIZE);
            System.out.printf("Sorting time is [%s] ms\n", sortingTime);
            System.out.printf("Parallel Sorting time is [%s] ms\n", parallelSortingTime);
//        System.out.printf("Better [%f] %%\n", ((sortingTime - parallelSortingTime) * 1.0 / sortingTime) * 100);
            System.out.printf("Naive Parallel Sorting time is [%s] ms\n", naiveParallelSortingTime);
        }
    }

    private static void sort() throws IOException {
        Deque<String> deque = new ArrayDeque<>();
        int index = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(Constants.BIG_INPUT_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String name = Util.randomUUID();

                deque.addLast(name);
                System.out.printf("%d. Writing sorted %s file\n", ++index, name);

                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);
                int j = 1;
                do {
                    toBeSorted.add(line);
                } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);

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
        try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(Constants.BIG_INPUT_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {

                final String name = Util.randomUUID();

                list.add(name);
                System.out.printf("%d. Writing sorted %s file\n", index++, name);

                int count = 0;
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    int j = 1;
                    do {
                        writer.append(line).append('\n');
                        count++;
                    } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);
                }
                System.out.printf("Count = %d\n", count);
            }
        }

        System.out.println("Name: " + ForkJoinPool.commonPool().invoke(new MergeSort<String>(list)));
        System.out.println("File was sorted!");
    }

    public static final class MergeSort<N extends Comparable<N>> extends RecursiveTask<String> {

        private final List<String> fileNames;

        public MergeSort(List<String> elements) {
            this.fileNames = new ArrayList<>(elements);
        }

        @Override
        protected String compute() {
            if (this.fileNames.size() <= 1) {
                final List<String> toBeSorted = new ArrayList<>();
                final String fileName = TEMP_FILES_FOLDER + this.fileNames.get(0);
                try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(fileName)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        toBeSorted.add(line);
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

    private static void naiveParallelSort() throws IOException {
        Deque<String> deque = new ArrayDeque<>();
        int index = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(Constants.BIG_INPUT_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final String name = Util.randomUUID();

                deque.addLast(name);
                System.out.printf("%d. Writing sorted %s file\n", ++index, name);

                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);
                int j = 1;
                do {
                    toBeSorted.add(line);
                } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);

                toBeSorted.sort(Comparator.naturalOrder());
                System.out.printf("Count = %d\n", toBeSorted.size());
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    for (String s : toBeSorted) {
                        writer.append(s).append('\n');
                    }
                }
            }
        }

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);
        Supplier<Runnable> supplier = () -> () -> {
            while (true) {
                String first;
                String second;
                synchronized (deque) {
                    if (deque.size() > 1) {
                        first = deque.removeFirst();
                        second = deque.removeFirst();
                    } else {
                        return;
                    }
                }
                String res = Util.randomUUID();

                try {
                    merge(TEMP_FILES_FOLDER + first, TEMP_FILES_FOLDER + second, TEMP_FILES_FOLDER + res);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                synchronized (deque) {
                    deque.addLast(res);
                }
            }
        };
        for (int i = 0; i < availableProcessors; i++) {
            executorService.submit(supplier.get());
        }
        executorService.shutdown();

        try {
            executorService.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static void merge(String first, String second, String res) throws IOException {
        System.out.printf(" Merging %s and %s to new created %s file \n", first, second, res);

        System.out.printf(" Used processors %d from %d availableProcessors \n", usedProcessors.incrementAndGet(), availableProcessors);

        try (
                BufferedReader scannerFirst = new BufferedReader(new FileReader(FileUtil.file(first)));
                BufferedReader scannerSecond = new BufferedReader(new FileReader(FileUtil.file(second)));
                Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(res)));
        ) {
            String valFirst = scannerFirst.readLine();
            String valSecond = scannerSecond.readLine();
            int counter = 0;
            while (valFirst != null || valSecond != null) {
                if (valSecond == null || (valFirst != null && valFirst.compareTo(valSecond) < 0)) {
                    writer.append(valFirst).append('\n');
                    valFirst = scannerFirst.readLine();
                } else {
                    writer.append(valSecond).append('\n');
                    valSecond = scannerSecond.readLine();
                }
                counter++;
            }

            FileUtil.removeFile(first);
            FileUtil.removeFile(second);

            System.out.printf("Count = %d\n", counter);
            usedProcessors.decrementAndGet();
        }
    }
}