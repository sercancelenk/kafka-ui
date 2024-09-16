package com.trendyol.kafka.stream.api.domain;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.springframework.http.HttpStatus;

public class Exceptions {

    public static class ProcessExecutionException extends RuntimeException {
        public ProcessExecutionException(Exception e) {
            super(e);
        }
    }

    public static class ChangeOffsetTimestampNotAcceptable extends BaseException {
        @Builder
        public ChangeOffsetTimestampNotAcceptable(Object[] params) {
            super("E104", params, HttpStatus.BAD_REQUEST);
        }
    }
    public static class ClusterNotSupportedException extends BaseException {
        @Builder
        public ClusterNotSupportedException(Object[] params) {
            super("E103", params, HttpStatus.NOT_FOUND);
        }
    }

    public static class TopicNotFoundException extends BaseException {
        @Builder
        public TopicNotFoundException(Object[] params) {
            super("E101", params, HttpStatus.NOT_FOUND);
        }
    }

    public static class MissingHeaderException extends BaseException {
        @Builder
        public MissingHeaderException(String... params) {
            super("E102", params, HttpStatus.NOT_ACCEPTABLE);
        }
    }


    @Getter
    public static class BaseException extends RuntimeException{
        private final HttpStatus status;
        private final String code;
        private final Object[] params;

        public BaseException(String code, Object[] params, HttpStatus status) {
            this.code = code;
            this.status = status;
            this.params = params;
        }
    }

    @Data
    @Builder
    public static class ErrorResponse {
        private String code;
        private String message;
    }

    public static class GenericException extends BaseException{
        public GenericException() {
            super("E100", null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
