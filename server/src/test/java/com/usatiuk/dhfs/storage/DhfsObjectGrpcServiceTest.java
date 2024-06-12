package com.usatiuk.dhfs.storage;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.objects.api.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

@QuarkusTest
class DhfsObjectGrpcServiceTest extends SimpleFileRepoTest {
    @GrpcClient
    DhfsObjectGrpc dhfsObjectGrpc;

    @ConfigProperty(name = "dhfs.filerepo.root")
    String tempDirectory;

    @Test
    void writeReadTest() {
        dhfsObjectGrpc.createNamespace(
                        CreateNamespaceRequest.newBuilder().setNamespace("testns").build())
                .await().atMost(Duration.ofSeconds(5));
        dhfsObjectGrpc.writeObject(
                        WriteObjectRequest.newBuilder().setNamespace("testns").setName("cool_file")
                                .setData(ByteString.copyFrom("Hello world".getBytes())).build())
                .await().atMost(Duration.ofSeconds(5));
        var read = dhfsObjectGrpc.readObject(
                        ReadObjectRequest.newBuilder().setNamespace("testns").setName("cool_file").build())
                .await().atMost(Duration.ofSeconds(5));
        Assertions.assertArrayEquals(read.getData().toByteArray(), "Hello world".getBytes());
        var found = dhfsObjectGrpc.findObjects(FindObjectsRequest.newBuilder().setNamespace("testns").build())
                .await().atMost(Duration.ofSeconds(5));
        Assertions.assertIterableEquals(found.getFoundList().stream().map(l -> l.getName()).toList(), List.of("cool_file"));
    }

}
