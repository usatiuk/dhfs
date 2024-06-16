package com.usatiuk.dhfs.storage.objects.api;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class DhfsObjectGrpcService implements DhfsObjectGrpc {
    @Inject
    ObjectRepository objectRepository;

    @Override
    public Uni<FindObjectsReply> findObjects(FindObjectsRequest request) {
        return objectRepository.findObjects(request.getPrefix())
                .map(m -> FindObjectsReply.FindObjectsEntry.newBuilder().setName(m).build())
                .collect().in(FindObjectsReply::newBuilder, FindObjectsReply.Builder::addFound)
                .map(FindObjectsReply.Builder::build);
    }

    @Override
    @RunOnVirtualThread
    public Uni<ReadObjectReply> readObject(ReadObjectRequest request) {
        var read = objectRepository.readObject(request.getName());
        return Uni.createFrom().item(ReadObjectReply.newBuilder().setData(ByteString.copyFrom(read)).build());
    }

    @Override
    @RunOnVirtualThread
    public Uni<WriteObjectReply> writeObject(WriteObjectRequest request) {
        objectRepository.writeObject(request.getName(), request.getData().toByteArray(), false);
        return Uni.createFrom().item(WriteObjectReply.newBuilder().build());
    }

    @Override
    @RunOnVirtualThread
    public Uni<DeleteObjectReply> deleteObject(DeleteObjectRequest request) {
        objectRepository.deleteObject(request.getName());
        return Uni.createFrom().item(DeleteObjectReply.newBuilder().build());
    }
}
