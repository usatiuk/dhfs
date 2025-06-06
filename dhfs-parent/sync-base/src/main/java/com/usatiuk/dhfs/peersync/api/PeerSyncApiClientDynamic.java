package com.usatiuk.dhfs.peersync.api;

import com.usatiuk.dhfs.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.peerdiscovery.PeerAddress;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Allows to query peers about their information, even if peer isn't part of the cluster.
 */
@ApplicationScoped
public class PeerSyncApiClientDynamic {

    @ConfigProperty(name = "dhfs.objects.sync.peer-sync-api.timeout", defaultValue = "5")
    int timeout;

    /**
     * Queries peer about its information.
     *
     * @param addr the address of the peer to query
     * @return the peer information
     */
    public ApiPeerInfo getSelfInfo(PeerAddress addr) {
        return switch (addr) {
            case IpPeerAddress ipAddr -> getSelfInfo(ipAddr.address().getHostAddress(), ipAddr.port());
            default -> throw new IllegalArgumentException("Unsupported peer address type: " + addr.getClass());
        };
    }

    private ApiPeerInfo getSelfInfo(String address, int port) {
        var client = QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create("http://" + address + ":" + port))
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .build(PeerSyncApiClient.class);
        return client.getSelfInfo();
    }
}
