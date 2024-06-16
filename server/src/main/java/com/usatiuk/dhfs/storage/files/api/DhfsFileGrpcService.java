package com.usatiuk.dhfs.storage.files.api;

import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;

@GrpcService
public class DhfsFileGrpcService implements DhfsFilesGrpc {
    @Override
    @Blocking
    public Uni<FindFilesReply> findFiles(FindFilesRequest request) {
        return null;
    }

    @Override
    @Blocking
    public Uni<ReadFileReply> readFile(ReadFileRequest request) {
        return null;
    }

    @Override
    @Blocking
    public Uni<WriteFileReply> writeFile(WriteFileRequest request) {
        return null;
    }

    @Override
    @Blocking
    public Uni<DeleteFileReply> deleteFile(DeleteFileRequest request) {
        return null;
    }
}
