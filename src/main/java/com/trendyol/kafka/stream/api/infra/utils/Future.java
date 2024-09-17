package com.trendyol.kafka.stream.api.infra.utils;

import com.trendyol.kafka.stream.api.domain.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class Future {
    public static <T> T call(KafkaFuture<T> future, Object format, String[]... arguments) {
        long startTime = System.currentTimeMillis();
        T call;

        try {
            call = future.get();
            log.debug("{} ms -> " + format.toString(), (System.currentTimeMillis() - startTime), arguments);
            return call;
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            if (Optional.ofNullable(e.getCause()).isPresent() && e.getCause() instanceof ApiException) {
                throw (ApiException) e.getCause();
            }
            throw new Exceptions.ProcessExecutionException(e);
        } catch (Exception exception) {
            log.error("Future call generic exception {}", exception.getMessage(), exception);
            throw new Exceptions.GenericException();
        }
    }

    public static <T> T call(KafkaFuture<T> future) {
        long startTime = System.currentTimeMillis();
        T call;

        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ApiException) {
                throw (ApiException) e.getCause();
            }
            throw new Exceptions.ProcessExecutionException(e);
        } catch (Exception exception) {
            throw new RuntimeException("Error for " + exception.getMessage(), exception);
        }
    }

    public static <I, R> List<R> forkJoin(Function<I, R> func, List<I> input) {

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Parallel processing using virtual threads
            return input.stream()
                    .map(inp -> CompletableFuture.supplyAsync(() -> {
                        R execute = null;
                        try {
                            execute = func.apply(inp);
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

    public static <I> void forkJoin(Consumer<I> func, List<I> input) {
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
