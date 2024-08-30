package com.usatiuk.dhfs.objects.protoserializer;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.files.objects.ChunkData;
import com.usatiuk.dhfs.files.objects.Directory;
import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeOpLog;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.persistence.*;
import com.usatiuk.dhfs.objects.repository.peersync.PeerDirectory;
import com.usatiuk.dhfs.objects.repository.peersync.PersistentPeerInfo;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Optional;

@Singleton
public class JObjectDataSerializer implements ProtoSerializer<JObjectDataP, JObjectData> {
    @Inject
    ProtoSerializer<FileP, File> fileProtoSerializer;
    @Inject
    ProtoSerializer<DirectoryP, Directory> directoryProtoSerializer;
    @Inject
    ProtoSerializer<ChunkDataP, ChunkData> chunkDataProtoSerializer;
    @Inject
    ProtoSerializer<PeerDirectoryP, PeerDirectory> peerDirectoryProtoSerializer;
    @Inject
    ProtoSerializer<PersistentPeerInfoP, PersistentPeerInfo> persistentPeerInfoProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeNodeP, JKleppmannTreeNode> jKleppmannTreeNodeProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreePersistentDataP, JKleppmannTreePersistentData> jKleppmannTreePersistentDataProtoSerializer;
    @Inject
    ProtoSerializer<JKleppmannTreeOpLogP, JKleppmannTreeOpLog> jKleppmannTreeOpLogProtoSerializer;

    @Override
    public JObjectData deserialize(JObjectDataP message) {
        return switch (message.getObjCase()) {
            case FILE -> fileProtoSerializer.deserialize(message.getFile());
            case DIRECTORY -> directoryProtoSerializer.deserialize(message.getDirectory());
            case CHUNKDATA -> chunkDataProtoSerializer.deserialize(message.getChunkData());
            case PEERDIRECTORY -> peerDirectoryProtoSerializer.deserialize(message.getPeerDirectory());
            case PERSISTENTPEERINFO -> persistentPeerInfoProtoSerializer.deserialize(message.getPersistentPeerInfo());
            case TREENODE -> jKleppmannTreeNodeProtoSerializer.deserialize(message.getTreeNode());
            case KLEPPMANNTREEPERSISTENTDATA ->
                    jKleppmannTreePersistentDataProtoSerializer.deserialize(message.getKleppmannTreePersistentData());
            case KLEPPMANNTREEOPLOG -> jKleppmannTreeOpLogProtoSerializer.deserialize(message.getKleppmannTreeOpLog());
            case OBJ_NOT_SET -> throw new IllegalStateException("Type not set when deserializing");
        };
    }

    // FIXME: This is annoying
    private <O> Optional<JObjectDataP> serializeToJObjectDataPInternal(O object) {
        if (object instanceof File f) {
            return Optional.of(JObjectDataP.newBuilder().setFile(
                    fileProtoSerializer.serialize(f)
            ).build());
        } else if (object instanceof Directory d) {
            return Optional.of(JObjectDataP.newBuilder().setDirectory(
                    directoryProtoSerializer.serialize(d)
            ).build());
        } else if (object instanceof ChunkData cd) {
            return Optional.of(JObjectDataP.newBuilder().setChunkData(
                    chunkDataProtoSerializer.serialize(cd)
            ).build());
        } else if (object instanceof PeerDirectory pd) {
            return Optional.of(JObjectDataP.newBuilder().setPeerDirectory(
                    peerDirectoryProtoSerializer.serialize(pd)
            ).build());
        } else if (object instanceof PersistentPeerInfo ppi) {
            return Optional.of(JObjectDataP.newBuilder().setPersistentPeerInfo(
                    persistentPeerInfoProtoSerializer.serialize(ppi)
            ).build());
        } else if (object instanceof JKleppmannTreeNode n) {
            return Optional.of(JObjectDataP.newBuilder().setTreeNode(
                    jKleppmannTreeNodeProtoSerializer.serialize(n)
            ).build());
        } else if (object instanceof JKleppmannTreePersistentData pd) {
            return Optional.of(JObjectDataP.newBuilder().setKleppmannTreePersistentData(
                    jKleppmannTreePersistentDataProtoSerializer.serialize(pd)
            ).build());
        } else if (object instanceof JKleppmannTreeOpLog l) {
            return Optional.of(JObjectDataP.newBuilder().setKleppmannTreeOpLog(
                    jKleppmannTreeOpLogProtoSerializer.serialize(l)
            ).build());
        } else {
            return Optional.empty();
        }
    }

    public JObjectDataP serialize(JObjectData object) {
        if (object == null) throw new IllegalArgumentException("Object to serialize shouldn't be null");

        return serializeToJObjectDataPInternal(object).orElseThrow(() -> new IllegalStateException("Unknown JObjectDataP type: " + object.getClass()));
    }
}
