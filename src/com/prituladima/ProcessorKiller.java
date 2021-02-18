package com.prituladima;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class ProcessorKiller {

    public static void main(String[] args) {

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);
        Supplier<Runnable> supplier = () -> () -> {
            while (true) {
            }
        };
        for (int i = 0; i < availableProcessors; i++) {
            executorService.submit(supplier.get());
        }
        executorService.shutdown();

    }

}
