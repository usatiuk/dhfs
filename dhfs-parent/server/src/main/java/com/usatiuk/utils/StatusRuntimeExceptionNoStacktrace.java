package com.usatiuk.utils;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;

public class StatusRuntimeExceptionNoStacktrace extends StatusRuntimeException {
    public StatusRuntimeExceptionNoStacktrace(Status status) {
        super(status);
    }

    public StatusRuntimeExceptionNoStacktrace(Status status, @Nullable Metadata trailers) {
        super(status, trailers);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
