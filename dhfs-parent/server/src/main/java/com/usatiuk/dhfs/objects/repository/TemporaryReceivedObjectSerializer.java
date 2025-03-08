package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.ReceivedObject;
import com.usatiuk.dhfs.objects.persistence.JDataRemoteP;
import com.usatiuk.dhfs.objects.persistence.JObjectKeyP;
import com.usatiuk.dhfs.objects.persistence.PeerIdP;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

@Singleton
public class TemporaryReceivedObjectSerializer implements ProtoSerializer<GetObjectReply, ReceivedObject> {
    @Inject
    ProtoSerializer<JDataRemoteP, JDataRemote> remoteObjectSerializer;

    @Override
    public ReceivedObject deserialize(GetObjectReply message) {
        PMap<PeerId, Long> changelog = HashTreePMap.empty();
        for (var entry : message.getChangelog().getEntriesList()) {
            changelog = changelog.plus(PeerId.of(entry.getKey().getId().getName()), entry.getValue());
        }
        var data = remoteObjectSerializer.deserialize(message.getPushedData());
        return new ReceivedObject(changelog, data);
    }

    @Override
    public GetObjectReply serialize(ReceivedObject object) {
        var builder = GetObjectReply.newBuilder();
        var changelogBuilder = builder.getChangelogBuilder();
        object.changelog().forEach((peer, version) -> {
            changelogBuilder.addEntriesBuilder()
                    .setKey(PeerIdP.newBuilder().setId(JObjectKeyP.newBuilder().setName(peer.id().toString()).build()).build())
                    .setValue(version);
        });
        builder.setPushedData(remoteObjectSerializer.serialize(object.data()));
        return builder.build();
    }
}
