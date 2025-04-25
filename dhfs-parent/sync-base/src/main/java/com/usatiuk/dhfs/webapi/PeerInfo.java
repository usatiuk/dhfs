package com.usatiuk.dhfs.webapi;

import jakarta.annotation.Nullable;

public record PeerInfo(String uuid, String cert, @Nullable String knownAddress) {
}
