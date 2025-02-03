package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.persistence.FileP;
import jakarta.enterprise.context.ApplicationScoped;
import org.pcollections.TreePMap;

@ApplicationScoped
public class FileSerializer implements ProtoSerializer<FileP, File> {
    @Override
    public File deserialize(FileP message) {
        TreePMap<Long, JObjectKey> chunks = TreePMap.empty();
        for (var chunk : message.getChunksList()) {
            chunks = chunks.plus(chunk.getStart(), JObjectKey.of(chunk.getId()));
        }
        var ret = new File(JObjectKey.of(message.getUuid()),
                message.getMode(),
                message.getCtime(),
                message.getMtime(),
                chunks,
                message.getSymlink(),
                message.getSize()
        );
        return ret;
    }

    @Override
    public FileP serialize(File object) {
        var builder = FileP.newBuilder()
                .setUuid(object.key().toString())
                .setMode(object.mode())
                .setCtime(object.cTime())
                .setMtime(object.mTime())
                .setSymlink(object.symlink())
                .setSize(object.size());
        object.chunks().forEach((s, i) -> {
            builder.addChunksBuilder()
                    .setStart(s)
                    .setId(i.toString());
        });
        return builder.build();
    }
}
