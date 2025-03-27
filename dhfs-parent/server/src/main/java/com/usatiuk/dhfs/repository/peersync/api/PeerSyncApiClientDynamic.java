package com.usatiuk.dhfs.repository.peersync.api;

import com.usatiuk.dhfs.repository.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.repository.peerdiscovery.PeerAddress;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PeerSyncApiClientDynamic {
    public ApiPeerInfo getSelfInfo(PeerAddress addr) {
        return switch (addr) {
            case IpPeerAddress ipAddr -> getSelfInfo(ipAddr.address().getHostAddress(), ipAddr.port());
            default -> throw new IllegalArgumentException("Unsupported peer address type: " + addr.getClass());
        };
    }

    private ApiPeerInfo getSelfInfo(String address, int port) {
        var client = QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create("http://" + address + ":" + port))
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build(PeerSyncApiClient.class);
        return client.getSelfInfo();
    }
}
