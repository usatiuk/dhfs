package com.usatiuk.dhfs.objects;

import org.pcollections.PMap;

public record ReceivedObject(JObjectKey key, PMap<PeerId, Long> changelog, JDataRemote data) {
}
