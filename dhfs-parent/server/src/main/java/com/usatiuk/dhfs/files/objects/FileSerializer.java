package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.FileP;
import com.usatiuk.dhfs.objects.persistence.FsNodeP;
import jakarta.inject.Singleton;

import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class FileSerializer implements ProtoSerializer<FileP, File> {
    @Override
    public File deserialize(FileP message) {
        TreeMap<Long, String> chunks = new TreeMap<>();
        message.getChunksList().forEach(chunk -> {
            chunks.put(chunk.getStart(), chunk.getId());
        });
        var ret = new File(UUID.fromString(message.getFsNode().getUuid()),
                message.getFsNode().getMode(),
                message.getSymlink(),
                chunks
        );
        ret.setMtime(message.getFsNode().getMtime());
        ret.setCtime(message.getFsNode().getCtime());
        ret.setSize(message.getSize());
        return ret;
    }

    @Override
    public FileP serialize(File object) {
        var builder = FileP.newBuilder()
                .setFsNode(FsNodeP.newBuilder()
                        .setCtime(object.getCtime())
                        .setMtime(object.getMtime())
                        .setMode(object.getMode())
                        .setUuid(object.getUuid().toString())
                        .build())
                .setSymlink(object.isSymlink())
                .setSize(object.getSize());
        object.getChunks().forEach((s, i) -> {
            builder.addChunksBuilder().setStart(s).setId(i);
        });
        return builder.build();
    }
}
