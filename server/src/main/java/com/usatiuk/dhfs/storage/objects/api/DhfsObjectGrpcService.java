package com.usatiuk.dhfs.storage.objects.api;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.objects.data.Namespace;
import com.usatiuk.dhfs.storage.objects.data.Object;
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
        return objectRepository.findObjects(request.getNamespace(), request.getPrefix())
                .map(m -> FindObjectsReply.FindObjectsEntry.newBuilder().setName(m).build())
                .collect().in(FindObjectsReply::newBuilder, FindObjectsReply.Builder::addFound)
                .map(FindObjectsReply.Builder::build);
    }

    @Override
    @RunOnVirtualThread
    public Uni<ReadObjectReply> readObject(ReadObjectRequest request) {
        var read = objectRepository.readObject(request.getNamespace(), request.getName());
        return Uni.createFrom().item(ReadObjectReply.newBuilder().setData(ByteString.copyFrom(read.getData())).build());
    }

    @Override
    @RunOnVirtualThread
    public Uni<WriteObjectReply> writeObject(WriteObjectRequest request) {
        objectRepository.writeObject(request.getNamespace(),
                new Object(new Namespace(request.getNamespace()), request.getName(), request.getData().toByteArray()), false);
        return Uni.createFrom().item(WriteObjectReply.newBuilder().build());
    }

    @Override
    @RunOnVirtualThread
    public Uni<DeleteObjectReply> deleteObject(DeleteObjectRequest request) {
        objectRepository.deleteObject(request.getNamespace(), request.getName());
        return Uni.createFrom().item(DeleteObjectReply.newBuilder().build());
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
