package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.List;

/**
 * Information about a new version of a remote object, possibly with its data.
 * @param key the key of the object
 * @param changelog the changelog of the object (version vector)
 * @param data the data of the object
 */
public record IndexUpdateOp(JObjectKey key, PMap<PeerId, Long> changelog, JDataRemoteDto data) implements Op {
    @Override
    public Collection<JObjectKey> getEscapedRefs() {
        return List.of(key);
    }
}
