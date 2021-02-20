package com.prituladima;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.prituladima.Constants.*;

public class BigFileSorter {

    static {
        System.loadLibrary("native");
    }

    public final static int availableProcessors = Runtime.getRuntime().availableProcessors();
    public final static AtomicInteger usedProcessors = new AtomicInteger(0);
    public final static PrintStream System_out = debug ? System.out : new PrintStream(new OutputStream(){
        @Override
        public void write(int b) throws IOException {
            //ignore
        }
    });
    //Use -Xmx64m to limit Heap size to 64 mb
    //-XX:ActiveProcessorCount=16
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        final Set<String> argsSet = new TreeSet<>(Arrays.asList(args));
        PrintStream out = new PrintStream(String.format("results%s.txt", new Date()));
        int tests = 1;
        for (int i = 0; i < tests; i++) {
            long sortingTime = -1;
            long parallelSortingTime = -1;
            long naiveParallelSortingTime = -1;
            long partitionParallelSortTime = -1;
            long nativeCodeTime = -1;
            if (argsSet.contains("sorting")) {
                long start = System.currentTimeMillis();
                sort();
                long end = System.currentTimeMillis();
                sortingTime = end - start;
            }
            if (argsSet.contains("parallelSorting")) {
                long start = System.currentTimeMillis();
                parallelSort();
                long end = System.currentTimeMillis();
                parallelSortingTime = end - start;
            }
            if (argsSet.contains("naiveParallelSorting")) {
                long start = System.currentTimeMillis();
                naiveParallelSort();
                long end = System.currentTimeMillis();
                naiveParallelSortingTime = end - start;
            }
            if (argsSet.contains("partitionParallelSort")) {
                long start = System.currentTimeMillis();
                partitionParallelSort();
                long end = System.currentTimeMillis();
                partitionParallelSortTime = end - start;
            }
            if (argsSet.contains("nativeCode")) {
                long start = System.currentTimeMillis();
                nativeCodeAllowed = true;
                parallelSort();
                nativeCodeAllowed = false;
                long end = System.currentTimeMillis();
                nativeCodeTime = end - start;
            }
//        System_out.printf("File size is %d strings\n", FILE_SIZE);
            out.printf("Sorting time is [%s] ms\n", sortingTime);
            out.printf("Parallel Sorting time is [%s] ms\n", parallelSortingTime);
//        System_out.printf("Better [%f] %%\n", ((sortingTime - parallelSortingTime) * 1.0 / sortingTime) * 100);
            out.printf("Naive Parallel Sorting time is [%s] ms\n", naiveParallelSortingTime);
            out.printf("Partition Parallel Sorting time is [%s] ms\n", partitionParallelSortTime);
            out.printf("Native Code Parallel Sorting time is [%s] ms\n", nativeCodeTime);
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
                System_out.printf("%d. Writing sorted %s file\n", ++index, name);

                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);
                int j = 1;
                do {
                    toBeSorted.add(line);
                } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);

                toBeSorted.sort(Comparator.naturalOrder());
                System_out.printf("Count = %d\n", toBeSorted.size());
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
        System_out.println("File was sorted!");

    }


    private static void parallelSort() throws IOException {
        List<String> list = new ArrayList<>();
        int index = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(Constants.BIG_INPUT_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {

                final String name = Util.randomUUID();

                list.add(name);
                System_out.printf("%d. Writing sorted %s file\n", index++, name);

                int count = 0;
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    int j = 1;
                    do {
                        writer.append(line).append('\n');
                        count++;
                    } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);
                }
                System_out.printf("Count = %d\n", count);
            }
        }

        System_out.println("Name: " + ForkJoinPool.commonPool().invoke(new MergeSort<String>(list, true)));
        System_out.println("File was sorted!");
    }

    public static final class MergeSort<N extends Comparable<N>> extends RecursiveTask<String> {

        private final List<String> fileNames;
        private final boolean sort;

        public MergeSort(List<String> elements, boolean sort) {
            this.fileNames = new ArrayList<>(elements);
            this.sort = sort;
        }

        @Override
        protected String compute() {
            if (this.fileNames.size() <= 1) {
                if (sort) {
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
                }
                return this.fileNames.get(0);
            } else {
                final int pivot = this.fileNames.size() / 2;
                MergeSort<N> leftTask = new MergeSort<>(this.fileNames.subList(0, pivot), sort);
                MergeSort<N> rightTask = new MergeSort<>(this.fileNames.subList(pivot, this.fileNames.size()), sort);

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
                System_out.printf("%d. Writing sorted %s file\n", ++index, name);

                List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);
                int j = 1;
                do {
                    toBeSorted.add(line);
                } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);

                toBeSorted.sort(Comparator.naturalOrder());
                System_out.printf("Count = %d\n", toBeSorted.size());
                try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                    for (String s : toBeSorted) {
                        writer.append(s).append('\n');
                    }
                }
            }
        }

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

    private static void partitionParallelSort() throws IOException, InterruptedException, ExecutionException {
        CountDownLatch[] chunksReady = new CountDownLatch[availableProcessors];
        for (int i = 0; i < availableProcessors; i++) {
            chunksReady[i] = new CountDownLatch(1);
        }

        final int expectedAmountOfChunks = (FILE_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE; //(FILE_SIZE / CHUNK_SIZE) ceiling division
        final int maxChunksForPartition = expectedAmountOfChunks / (availableProcessors - 1); //Last core always get less - possible performance leak
        final String[] chunkList = new String[expectedAmountOfChunks];

        ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);
        Future<Object> splitOnChunksFuture = executorService.submit(() -> {
            int chunkListIndex = 0;
            int index = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(FileUtil.file(BIG_INPUT_FILE)))) {
                int coreIndex = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    final String name = Util.randomUUID();

                    chunkList[chunkListIndex++] = name;
                    System_out.printf("%d. Writing sorted %s file\n", ++index, name);

                    List<String> toBeSorted = new ArrayList<>(CHUNK_SIZE);
                    int j = 1;
                    do {
                        toBeSorted.add(line);
                    } while (j++ < CHUNK_SIZE && (line = reader.readLine()) != null);

                    toBeSorted.sort(Comparator.naturalOrder());
                    System_out.printf("Count = %d\n", toBeSorted.size());
                    try (Writer writer = new BufferedWriter(new FileWriter(FileUtil.createFile(TEMP_FILES_FOLDER + name)))) {
                        for (String s : toBeSorted) {
                            writer.append(s).append('\n');
                        }
                    }
                    int chunkOffset = coreIndex * maxChunksForPartition;
                    int upTo = Math.min(chunkOffset + maxChunksForPartition, expectedAmountOfChunks);
                    if (chunkListIndex == upTo) {
                        System_out.printf("Chunk [%d,%d) is ready! Producing chunk batch for core = [%d]\n", chunkOffset, upTo, coreIndex);
                        chunksReady[coreIndex++].countDown();

                    }
                }
            }
            return null;
        });

        IntFunction<Callable<String>> chunkNumberToRunnable = (coreIndex) -> () -> {

            int chunkOffset = coreIndex * maxChunksForPartition;
            int upTo = Math.min(chunkOffset + maxChunksForPartition, expectedAmountOfChunks);
            System_out.printf("Chunk [%d,%d) is ready! Consuming chunk batch for core = [%d]\n", chunkOffset, upTo, coreIndex);

            Deque<String> deque = new ArrayDeque<>();

            for (int i = chunkOffset; i < upTo; i++) {
                deque.add(chunkList[i]);
            }
            while (deque.size() > 1) {
                String first = deque.removeFirst();
                String second = deque.removeFirst();

                String res = Util.randomUUID();

                merge(TEMP_FILES_FOLDER + first, TEMP_FILES_FOLDER + second, TEMP_FILES_FOLDER + res);
                deque.addLast(res);
            }
            return deque.removeFirst();
        };


        List<Future<String>> futureParts = new ArrayList<>();
        for (int coreIndex = 0; coreIndex < availableProcessors; coreIndex++) {
            chunksReady[coreIndex].await();
            futureParts.add(executorService.submit(chunkNumberToRunnable.apply(coreIndex)));
        }
        executorService.shutdown();
        List<String> reducedList = new ArrayList<>();
        for (Future<String> futurePart : futureParts) {
            reducedList.add(futurePart.get());
        }
        ForkJoinPool.commonPool().invoke(new MergeSort<String>(reducedList, false));
    }

    private static void merge(String first, String second, String res) throws IOException {
        if (nativeCodeAllowed) {
            merge0(WORKING_FOLDER + first, WORKING_FOLDER + second, WORKING_FOLDER + res);
            return;
        }

        System_out.printf(" Merging %s and %s to new created %s file \n", first, second, res);

        System_out.printf(" Used processors %d from %d availableProcessors \n", usedProcessors.incrementAndGet(), availableProcessors);

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

            System_out.printf("Count = %d\n", counter);
            usedProcessors.decrementAndGet();
        }
    }

    private static native void merge0(String first, String second, String res);
}