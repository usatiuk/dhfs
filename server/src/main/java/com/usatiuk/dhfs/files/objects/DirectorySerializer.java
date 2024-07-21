package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.persistence.DirectoryP;
import com.usatiuk.dhfs.objects.persistence.FsNodeP;
import com.usatiuk.dhfs.objects.protoserializer.ProtoDeserializer;
import com.usatiuk.dhfs.objects.protoserializer.ProtoSerializer;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class DirectorySerializer implements ProtoSerializer<DirectoryP, Directory>, ProtoDeserializer<DirectoryP, Directory> {
    @Override
    public Directory deserialize(DirectoryP message) {
        var ret = new Directory(UUID.fromString(message.getFsNode().getUuid()));
        ret.setMtime(message.getFsNode().getMtime());
        ret.setCtime(message.getFsNode().getCtime());
        ret.setMode(message.getFsNode().getMode());
        ret.getChildren().putAll(message.getChildrenMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> UUID.fromString(e.getValue()))));
        return ret;
    }

    @Override
    public DirectoryP serialize(Directory object) {
        return DirectoryP.newBuilder()
                .setFsNode(FsNodeP.newBuilder()
                        .setCtime(object.getCtime())
                        .setMtime(object.getMtime())
                        .setMode(object.getMode())
                        .setUuid(object.getUuid().toString())
                        .build())
                .putAllChildren(object.getChildren().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())))
                .build();
    }
}
