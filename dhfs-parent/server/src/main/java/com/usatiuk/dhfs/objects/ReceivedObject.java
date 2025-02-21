package com.usatiuk.dhfs.objects;

import org.pcollections.PMap;

public record ReceivedObject(PMap<PeerId, Long> changelog, JDataRemote data) {
}
