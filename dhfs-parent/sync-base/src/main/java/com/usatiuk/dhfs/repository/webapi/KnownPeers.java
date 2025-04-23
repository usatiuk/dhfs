package com.usatiuk.dhfs.repository.webapi;

import java.util.List;

public record KnownPeers(List<KnownPeerInfo> peers, String selfUuid) {
}
