package com.usatiuk.dhfs.persistence;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ProtoSerializationTest {

//    @Inject
//    ProtoSerializerService protoSerializerService;
//
//    @Test
//    void SerializeDeserializePeerDirectory() {
//        var pd = new PeerDirectory();
//        pd.getPeers().add(UUID.randomUUID());
//        var ser = JObjectDataP.newBuilder().setPeerDirectory((PeerDirectoryP) protoSerializerService.serialize(pd)).build();
//        var deser = (PeerDirectory) protoSerializerService.deserialize(ser);
//        Assertions.assertIterableEquals(pd.getPeers(), deser.getPeers());
//
//        var ser2 = protoSerializerService.serializeToJObjectDataP(pd);
//        var deser2 = (PeerDirectory) protoSerializerService.deserialize(ser2);
//        Assertions.assertIterableEquals(pd.getPeers(), deser2.getPeers());
//    }
//
}
