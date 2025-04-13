package com.usatiuk.dhfs;

import com.usatiuk.dhfs.repository.JDataRemoteDto;
import org.pcollections.PMap;

public record ReceivedObject(PMap<PeerId, Long> changelog, JDataRemoteDto data) {
}
