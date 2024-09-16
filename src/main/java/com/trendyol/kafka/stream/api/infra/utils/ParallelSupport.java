package com.trendyol.kafka.stream.api.infra.utils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelSupport {

    public interface ExecuteFunc<I, R> {
        R execute(I input) throws Exception;
    }

    public static <I, R> List<R> forkJoinCollect(ExecuteFunc<I, R> func, List<I> input) {
        // Create an executor that uses virtual threads for each task
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        try {
            // Parallel processing using virtual threads
            return input.stream()
                    .map(inp -> CompletableFuture.supplyAsync(() -> {
                        R execute = null;
                        try {
                            execute = func.execute(inp);
                        } catch (Exception ex) {
                            System.err.println("Error occurred at execute function in ParallelSupport: " + inp);
                        }
                        return execute;
                    }, executor)).toList()
                    .stream()
                    .map(CompletableFuture::join)
                    .toList();
        } finally {
            executor.shutdown(); // Ensure the executor is shut down after use
        }
    }
}