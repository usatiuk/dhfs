package com.usatiuk.dhfs.storage;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.storage.objects.api.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class Profiles {
    public static class DhfsObjectGrpcServiceTestProfile implements QuarkusTestProfile {
    }
}

@QuarkusTest
@TestProfile(Profiles.DhfsObjectGrpcServiceTestProfile.class)
class DhfsObjectGrpcServiceTest {
    @GrpcClient
    DhfsObjectGrpc dhfsObjectGrpc;

    @Test
    void writeReadTest() {
        dhfsObjectGrpc.writeObject(
                        WriteObjectRequest.newBuilder().setName("cool_file")
                                .setData(ByteString.copyFrom("Hello world".getBytes())).build())
                .await().atMost(Duration.ofSeconds(5));
        var read = dhfsObjectGrpc.readObject(
                        ReadObjectRequest.newBuilder().setName("cool_file").build())
                .await().atMost(Duration.ofSeconds(5));
        Assertions.assertArrayEquals(read.getData().toByteArray(), "Hello world".getBytes());
//        var found = dhfsObjectGrpc.findObjects(FindObjectsRequest.newBuilder().setNamespace("testns").build())
//                .await().atMost(Duration.ofSeconds(5));
//        Assertions.assertIterableEquals(found.getFoundList().stream().map(l -> l.getName()).toList(), List.of("cool_file"));
    }

}
