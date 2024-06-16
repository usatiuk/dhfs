package com.usatiuk.dhfs.storage.objects.api;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class DhfsObjectGrpcService implements DhfsObjectGrpc {
    @Inject
    ObjectRepository objectRepository;

    @Override
    @Blocking
    public Uni<FindObjectsReply> findObjects(FindObjectsRequest request) {
        var objects = objectRepository.findObjects(request.getPrefix());
        var builder = FindObjectsReply.newBuilder();
        for (var obj : objects) {
            builder.addFound(FindObjectsReply.FindObjectsEntry.newBuilder().setName(obj).build());
        }
        return Uni.createFrom().item(builder.build());
    }

    @Override
    @Blocking
    public Uni<ReadObjectReply> readObject(ReadObjectRequest request) {
        var read = objectRepository.readObject(request.getName());
        return Uni.createFrom().item(ReadObjectReply.newBuilder().setData(ByteString.copyFrom(read)).build());
    }

    @Override
    @Blocking
    public Uni<WriteObjectReply> writeObject(WriteObjectRequest request) {
        objectRepository.writeObject(request.getName(), request.getData().toByteArray(), false);
        return Uni.createFrom().item(WriteObjectReply.newBuilder().build());
    }

    @Override
    @Blocking
    public Uni<DeleteObjectReply> deleteObject(DeleteObjectRequest request) {
        objectRepository.deleteObject(request.getName());
        return Uni.createFrom().item(DeleteObjectReply.newBuilder().build());
    }
}
