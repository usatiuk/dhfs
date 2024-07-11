package com.usatiuk.dhfs.objects.repository.peersync;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PeerSyncApiClientDynamic {
    public PeerInfo getSelfInfo(String addr, int port) {
        var client = QuarkusRestClientBuilder.newBuilder()
                .baseUri(URI.create("http://" + addr + ":" + port))
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build(PeerSyncApiClient.class);
        return client.getSelfInfo();
    }
}
