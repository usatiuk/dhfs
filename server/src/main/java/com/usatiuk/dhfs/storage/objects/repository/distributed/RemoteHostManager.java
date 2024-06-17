package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.DhfsObjectSyncGrpcGrpc;
import io.grpc.netty.NettyChannelBuilder;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.HashMap;

@ApplicationScoped
public class RemoteHostManager {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @ConfigProperty(name = "dhfs.objects.distributed.friend")
    String remoteHostName;
    @ConfigProperty(name = "dhfs.objects.distributed.friendAddr")
    String remoteHostAddr;
    @ConfigProperty(name = "dhfs.objects.distributed.friendPort")
    String remoteHostPort;

    @Getter
    @AllArgsConstructor
    private static class HostInfo {
        String _addr;
        Integer _port;
    }

    final HashMap<String, HostInfo> _remoteHosts = new HashMap<>();

    void init(@Observes @Priority(350) StartupEvent event) throws IOException {
        _remoteHosts.put(remoteHostName, new HostInfo(remoteHostAddr, Integer.valueOf(remoteHostPort)));
    }

    void shutdown(@Observes @Priority(250) ShutdownEvent event) throws IOException {
    }

    @FunctionalInterface
    public interface ClientFunction<R> {
        R apply(DhfsObjectSyncGrpcGrpc.DhfsObjectSyncGrpcBlockingStub client);
    }

    public <R> R withClient(ClientFunction<R> fn) {
        var hostInfo = _remoteHosts.get(remoteHostName);
        var channel = NettyChannelBuilder.forAddress(hostInfo.getAddr(), hostInfo.getPort())
                .usePlaintext().build();
        var client = DhfsObjectSyncGrpcGrpc.newBlockingStub(channel)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                .withMaxInboundMessageSize(Integer.MAX_VALUE);
        try {
            return fn.apply(client);
        } finally {
            channel.shutdownNow();
        }
    }
}
