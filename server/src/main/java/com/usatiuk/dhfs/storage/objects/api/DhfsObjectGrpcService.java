package com.usatiuk.dhfs.storage.objects.api;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.api.*;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.nio.ByteBuffer;

@GrpcService
public class DhfsObjectGrpcService implements DhfsObjectGrpc {
    @Inject
    ObjectRepository objectRepository;

    @Override
    public Uni<FindObjectsReply> findObjects(FindObjectsRequest request) {
        return objectRepository.findObjects(request.getNamespace(), request.getPrefix())
                .map(m -> FindObjectsReply.FindObjectsEntry.newBuilder().setName(m).build())
                .collect().in(FindObjectsReply::newBuilder, FindObjectsReply.Builder::addFound)
                .map(FindObjectsReply.Builder::build);
    }

    @Override
    public Uni<ReadObjectReply> readObject(ReadObjectRequest request) {
        return objectRepository.readObject(request.getNamespace(), request.getName())
                .map(n -> ReadObjectReply.newBuilder().setData(ByteString.copyFrom(n.getData())).build());
    }

    @Override
    public Uni<WriteObjectReply> writeObject(WriteObjectRequest request) {
        return objectRepository.writeObject(request.getNamespace(), request.getName(), ByteBuffer.wrap(request.getData().toByteArray()))
                .map(n -> WriteObjectReply.newBuilder().build());
    }

    @Override
    public Uni<DeleteObjectReply> deleteObject(DeleteObjectRequest request) {
        return objectRepository.deleteObject(request.getNamespace(), request.getName())
                .map(n -> DeleteObjectReply.newBuilder().build());
    }

    @Override
    public Uni<CreateNamespaceReply> createNamespace(CreateNamespaceRequest request) {
        return objectRepository.createNamespace(request.getNamespace())
                .map(n -> CreateNamespaceReply.newBuilder().build());
    }

    @Override
    public Uni<DeleteNamespaceReply> deleteNamespace(DeleteNamespaceRequest request) {
        return objectRepository.deleteNamespace(request.getNamespace())
                .map(n -> DeleteNamespaceReply.newBuilder().build());
    }
}
