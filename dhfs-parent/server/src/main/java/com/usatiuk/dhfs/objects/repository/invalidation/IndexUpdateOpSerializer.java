package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.repository.IndexUpdateOpP;
import jakarta.enterprise.context.ApplicationScoped;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

@ApplicationScoped
public class IndexUpdateOpSerializer implements ProtoSerializer<IndexUpdateOpP, IndexUpdateOp> {

    @Override
    public IndexUpdateOp deserialize(IndexUpdateOpP message) {
        PMap<PeerId, Long> map = HashTreePMap.empty();
        for (var entry : message.getHeader().getChangelog().getEntriesList()) {
            map = map.plus(PeerId.of(entry.getHost()), entry.getVersion());
        }
        return new IndexUpdateOp(JObjectKey.of(message.getHeader().getName()), map);
    }

    @Override
    public IndexUpdateOpP serialize(IndexUpdateOp object) {
        var builder = IndexUpdateOpP.newBuilder();
        var headerBuilder = builder.getHeaderBuilder();
        headerBuilder.setName(object.key().name());
        var changelogBuilder = headerBuilder.getChangelogBuilder();
        for (var entry : object.changelog().entrySet()) {
            var entryBuilder = changelogBuilder.addEntriesBuilder();
            entryBuilder.setHost(entry.getKey().id().toString());
            entryBuilder.setVersion(entry.getValue());
        }
        return builder.build();
    }
}
