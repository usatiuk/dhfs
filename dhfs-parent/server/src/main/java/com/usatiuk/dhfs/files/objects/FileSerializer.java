package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.persistence.FileP;
import com.usatiuk.dhfs.objects.persistence.FsNodeP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@ApplicationScoped
public class FileSerializer implements ProtoSerializer<FileP, File>, ProtoDeserializer<FileP, File> {
    @Override
    public File deserialize(FileP message) {
        var ret = new File(UUID.fromString(message.getFsNode().getUuid()), message.getFsNode().getMode(), message.getSymlink());
        ret.setMtime(message.getFsNode().getMtime());
        ret.setCtime(message.getFsNode().getCtime());
        ret.getChunks().putAll(message.getChunksMap());
        return ret;
    }

    @Override
    public FileP serialize(File object) {
        var ret = FileP.newBuilder()
                       .setFsNode(FsNodeP.newBuilder()
                                         .setCtime(object.getCtime())
                                         .setMtime(object.getMtime())
                                         .setMode(object.getMode())
                                         .setUuid(object.getUuid().toString())
                                         .build())
                       .putAllChunks(object.getChunks())
                       .setSymlink(object.isSymlink())
                       .build();
        return ret;
    }
}
