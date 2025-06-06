package com.usatiuk.dhfs.rpc;

import com.usatiuk.dhfs.ProtoSerializer;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.persistence.JDataRemoteDtoP;
import com.usatiuk.dhfs.persistence.JObjectKeyP;
import com.usatiuk.dhfs.persistence.PeerIdP;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.dhfs.remoteobj.ReceivedObject;
import com.usatiuk.dhfs.repository.GetObjectReply;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

@ApplicationScoped
public class ReceivedObjectSerializer implements ProtoSerializer<GetObjectReply, ReceivedObject> {
    @Inject
    ProtoSerializer<JDataRemoteDtoP, JDataRemoteDto> remoteObjectSerializer;

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
