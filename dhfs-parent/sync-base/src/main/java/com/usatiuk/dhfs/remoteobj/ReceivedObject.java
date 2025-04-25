package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PeerId;
import org.pcollections.PMap;

public record ReceivedObject(PMap<PeerId, Long> changelog, JDataRemoteDto data) {
}
