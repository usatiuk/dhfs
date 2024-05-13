package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.api.*;
import io.smallrye.mutiny.Uni;

public interface DhfsFileService {
    public Uni<FindFilesReply> findFiles(FindFilesRequest request);

    public Uni<ReadFileReply> readFile(ReadFileRequest request) ;

    public Uni<WriteFileReply> writeFile(WriteFileRequest request) ;

    public Uni<DeleteFileReply> deleteFile(DeleteFileRequest request) ;

}
