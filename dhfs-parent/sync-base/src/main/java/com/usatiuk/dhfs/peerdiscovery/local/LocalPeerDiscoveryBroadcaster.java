package com.usatiuk.dhfs.peerdiscovery.local;

import com.usatiuk.dhfs.peersync.PersistentPeerDataService;
import com.usatiuk.dhfs.peerdiscovery.PeerDiscoveryInfo;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.*;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.local-discovery", stringValue = "true")
public class LocalPeerDiscoveryBroadcaster {
    @Inject
    PersistentPeerDataService persistentPeerDataService;

    @ConfigProperty(name = "quarkus.http.port")
    int ourPort;

    @ConfigProperty(name = "quarkus.http.ssl-port")
    int ourSecurePort;

    @ConfigProperty(name = "dhfs.objects.peerdiscovery.port")
    int broadcastPort;

    @ConfigProperty(name = "dhfs.objects.peerdiscovery.broadcast")
    boolean enabled;

    private DatagramSocket _socket;

    @Startup
    void init() throws SocketException {
        if (!enabled) {
            return;
        }
        _socket = new DatagramSocket();
        _socket.setBroadcast(true);
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        if (!enabled) {
            return;
        }
        _socket.close();
    }

    @Scheduled(every = "${dhfs.objects.peerdiscovery.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void broadcast() throws Exception {
        if (!enabled) {
            return;
        }
        var sendData = PeerDiscoveryInfo.newBuilder()
                .setUuid(persistentPeerDataService.getSelfUuid().toString())
                .setPort(ourPort)
                .setSecurePort(ourSecurePort)
                .build();

        var sendBytes = sendData.toByteArray();

        DatagramPacket sendPacket
                = new DatagramPacket(sendBytes, sendBytes.length,
                InetAddress.getByName("255.255.255.255"), broadcastPort);

        _socket.send(sendPacket);

        var interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();

            try {
                if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                    continue;
                }
            } catch (Exception e) {
                continue;
            }

            for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                InetAddress broadcast = interfaceAddress.getBroadcast();
                if (broadcast == null) {
                    continue;
                }

                try {
                    sendPacket = new DatagramPacket(sendBytes, sendBytes.length, broadcast, broadcastPort);
                    _socket.send(sendPacket);
                    Log.tracev("Broadcast sent to: {0}, at: {1}", broadcast.getHostAddress(), networkInterface.getDisplayName());
                } catch (Exception ignored) {
                    continue;
                }

//                            Log.trace(getClass().getName() + "Broadcast sent to: " + broadcast.getHostAddress()
//                                    + ", at: " + networkInterface.getDisplayName());
            }
        }
    }
}
