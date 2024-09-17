package com.trendyol.kafka.stream.api.infra.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class ForkJoinSupport {

    public interface Func<I, R> {
        R execute(I input) throws Exception;
    }

    public static <I, R> List<R> execute(Func<I, R> func, List<I> input) {

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

    public static <I> void execute(Consumer<I> func, List<I> input) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            input.stream()
                    .map(inp -> CompletableFuture.runAsync(() -> {
                        try {
                            func.accept(inp);
                        } catch (Exception ex) {
                            log.error("Error occurred at execute function in ForkJoinSupport. {}", inp);
                        }
                    }, executor))
                    .toList()
                    .forEach(voidCompletableFuture -> {
                        log.debug("Completable future join step.");
                        voidCompletableFuture.join();
                    });
        }
    }
}