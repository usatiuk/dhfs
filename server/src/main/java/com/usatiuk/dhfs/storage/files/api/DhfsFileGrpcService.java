package com.usatiuk.dhfs.storage.files.api;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class DhfsFileGrpcService implements DhfsFilesGrpc {
    @Override
    public Uni<FindFilesReply> findFiles(FindFilesRequest request) {
        return null;
    }

    @Override
    public Uni<ReadFileReply> readFile(ReadFileRequest request) {
        return null;
    }

    @Override
    public Uni<WriteFileReply> writeFile(WriteFileRequest request) {
        return null;
    }

    @Override
    public Uni<DeleteFileReply> deleteFile(DeleteFileRequest request) {
        return null;
    }
}
