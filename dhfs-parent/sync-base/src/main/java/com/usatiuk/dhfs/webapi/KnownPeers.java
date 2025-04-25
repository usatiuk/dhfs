package com.usatiuk.dhfs.webapi;

import java.util.List;

public record KnownPeers(List<KnownPeerInfo> peers, String selfUuid) {
}
