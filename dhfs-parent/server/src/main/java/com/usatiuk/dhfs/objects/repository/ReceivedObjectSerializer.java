package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.ReceivedObject;
import com.usatiuk.dhfs.objects.persistence.RemoteObjectP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

@ApplicationScoped
public class ReceivedObjectSerializer implements ProtoSerializer<GetObjectReply, ReceivedObject> {
    @Inject
    ProtoSerializer<RemoteObjectP, JDataRemote> remoteObjectSerializer;

    @Override
    public ReceivedObject deserialize(GetObjectReply message) {
        PMap<PeerId, Long> changelog = HashTreePMap.empty();
        for (var entry : message.getHeader().getChangelog().getEntriesList()) {
            changelog = changelog.plus(PeerId.of(entry.getHost()), entry.getVersion());
        }
        return new ReceivedObject(
                JObjectKey.of(message.getHeader().getName()),
                changelog,
                remoteObjectSerializer.deserialize(message.getContent())
        );
    }

    @Override
    public GetObjectReply serialize(ReceivedObject object) {
        var builder = GetObjectReply.newBuilder();
        var headerBuilder = builder.getHeaderBuilder();
        headerBuilder.setName(object.key().toString());
        var changelogBuilder = headerBuilder.getChangelogBuilder();
        object.changelog().forEach((peer, version) -> {
            changelogBuilder.addEntriesBuilder()
                    .setHost(peer.toString())
                    .setVersion(version);
        });
        builder.setContent(remoteObjectSerializer.serialize(object.data()));
        return builder.build();
    }
}
