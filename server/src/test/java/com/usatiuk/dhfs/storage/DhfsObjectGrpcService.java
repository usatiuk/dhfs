package com.usatiuk.dhfs.storage;

import com.usatiuk.dhfs.storage.api.DhfsObjectGrpc;
import com.usatiuk.dhfs.storage.api.FindObjectsReply;
import com.usatiuk.dhfs.storage.api.FindObjectsRequest;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@QuarkusTest
class DhfsObjectGrpcService {
    @GrpcClient
    DhfsObjectGrpc dhfsObjectGrpc;

    @ConfigProperty(name = "dhfs.filerepo.root")
    String tempDirectory;

    @Test
    void testFind() {
        FindObjectsReply reply = dhfsObjectGrpc
                .findObjects(FindObjectsRequest.newBuilder().setNamespace("TestNs").build()).await().atMost(Duration.ofSeconds(5));
    }

}
