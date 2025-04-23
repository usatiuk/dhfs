package com.usatiuk.dhfs.repository.webapi;

import jakarta.annotation.Nullable;

public record KnownPeerInfo(String uuid, @Nullable String knownAddress) {
}
