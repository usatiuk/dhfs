package com.usatiuk.dhfs.storage.api;

import com.usatiuk.dhfs.storage.repository.ObjectRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

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
        return Uni.createFrom().item("Hello ")
                .map(msg -> ReadObjectReply.newBuilder().build());
    }

    @Override
    public Uni<WriteObjectReply> writeObject(WriteObjectRequest request) {
        return Uni.createFrom().item("Hello ")
                .map(msg -> WriteObjectReply.newBuilder().build());
    }
}
