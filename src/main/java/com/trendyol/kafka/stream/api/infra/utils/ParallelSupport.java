package com.trendyol.kafka.stream.api.infra.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ParallelSupport {

    public interface ExecuteFunc<I, R> {
        R execute(I input) throws Exception;
    }

    public static <I, R> List<R> forkJoinCollect(ExecuteFunc<I, R> func, List<I> input) {

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Parallel processing using virtual threads
            return input.stream()
                    .map(inp -> CompletableFuture.supplyAsync(() -> {
                        R execute = null;
                        try {
                            execute = func.execute(inp);
                        } catch (Exception ex) {
                            log.error("Error occurred at execute function. Input {}. Detail: {}", inp, ex.getMessage(), ex);
                        }
                        return execute;
                    }, executor)).toList()
                    .stream()
                    .map(CompletableFuture::join)
                    .toList();
        }
    }
}