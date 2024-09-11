package com.trendyol.kafka.stream.api.controller.filter;

import com.trendyol.kafka.stream.api.model.Exceptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

    @ExceptionHandler({ExecutionException.class, InterruptedException.class, TimeoutException.class})
    public ResponseEntity<Exceptions.ErrorResponse> handleExecutionException(Exception ex, Locale locale) {
        log.error("Error occurred when requesting. ", ex);
        Exceptions.ErrorResponse errorResponse = Exceptions.ErrorResponse.builder()
                .message("Generic error")
                .build();
        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }

}