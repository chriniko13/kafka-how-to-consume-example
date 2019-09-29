package com.chriniko.kafkaread.demonstration.core;

public class ProcessingException extends RuntimeException {

    public ProcessingException(Throwable e) {
        super(e);
    }
}
