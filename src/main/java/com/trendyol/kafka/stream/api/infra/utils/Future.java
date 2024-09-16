package com.trendyol.kafka.stream.api.infra.utils;

import com.trendyol.kafka.stream.api.domain.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

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
}
