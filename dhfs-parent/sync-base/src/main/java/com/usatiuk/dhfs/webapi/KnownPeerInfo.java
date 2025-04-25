package com.usatiuk.dhfs.webapi;

import jakarta.annotation.Nullable;

public record KnownPeerInfo(String uuid, @Nullable String knownAddress) {
}
