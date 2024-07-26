package com.usatiuk.dhfs.persistence;

import com.usatiuk.dhfs.objects.persistence.BlobP;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.PeerDirectoryP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializerService;
import com.usatiuk.dhfs.objects.repository.peersync.PeerDirectory;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

@QuarkusTest
public class ProtoSerializationTest {

    @Inject
    ProtoSerializerService protoSerializerService;

    @Test
    void SerializeDeserializePeerDirectory() {
        var pd = new PeerDirectory();
        pd.getPeers().add(UUID.randomUUID());
        var ser = BlobP.newBuilder().setData(JObjectDataP.newBuilder().setPeerDirectory((PeerDirectoryP) protoSerializerService.serialize(pd)).build()).build();
        var deser = (PeerDirectory) protoSerializerService.deserialize(ser);
        Assertions.assertIterableEquals(pd.getPeers(), deser.getPeers());
    }

}