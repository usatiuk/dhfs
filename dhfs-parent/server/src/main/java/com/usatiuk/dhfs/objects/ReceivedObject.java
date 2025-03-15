package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;
import org.pcollections.PMap;

public record ReceivedObject(PMap<PeerId, Long> changelog, JDataRemoteDto data) {
}
