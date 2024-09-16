package com.trendyol.kafka.stream.api.adapters.rest.filter;

import com.trendyol.kafka.stream.api.domain.Exceptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@RestControllerAdvice
@RequiredArgsConstructor
@Slf4j
public class GlobalExceptionHandler {
    private final MessageSource messageSource;

    @ExceptionHandler(Exceptions.BaseException.class)
    public ResponseEntity<Exceptions.ErrorResponse> handleBaseException(Exceptions.BaseException ex, Locale locale) {
        String localizedMessage = messageSource.getMessage(ex.getCode(), ex.getParams(), locale);
        Exceptions.ErrorResponse errorResponse = Exceptions.ErrorResponse.builder()
                .code(ex.getCode())
                .message(localizedMessage)
                .build();
        return new ResponseEntity<>(errorResponse, ex.getStatus());
    }

    @ExceptionHandler({Exceptions.ProcessExecutionException.class})
    public ResponseEntity<Exceptions.ErrorResponse> handleExecutionException(Exceptions.ProcessExecutionException ex, Locale locale) {
        log.error("Kafka Exception occurred. {}", ex.getMessage(), ex);

        Exceptions.ErrorResponse errorResponse = Exceptions.ErrorResponse.builder()
                .message("Generic error")
                .build();
        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }

}