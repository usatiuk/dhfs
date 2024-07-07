package com.usatiuk.dhfs.storage.objects.repository.peerdiscovery;

import com.usatiuk.dhfs.objects.repository.peerdiscovery.PeerDiscoveryInfo;
import com.usatiuk.dhfs.storage.objects.repository.PersistentRemoteHostsService;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.*;

@ApplicationScoped
public class LocalPeerDiscoveryBroadcaster {

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    @ConfigProperty(name = "quarkus.http.port")
    Integer ourPort;

    @ConfigProperty(name = "quarkus.http.ssl-port")
    Integer ourSecurePort;

    @ConfigProperty(name = "dhfs.objects.peerdiscovery.port")
    Integer broadcastPort;

    @ConfigProperty(name = "dhfs.objects.peerdiscovery.interval")
    Integer broadcastInterval;

    private Thread _broadcasterThread;

    private DatagramSocket _socket;

    @Startup
    void init() throws SocketException {
        _socket = new DatagramSocket();
        _socket.setBroadcast(true);

        _broadcasterThread = new Thread(this::broadcast);
        _broadcasterThread.setName("LocalPeerDiscoveryBroadcaster");
        _broadcasterThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _socket.close();
        _broadcasterThread.interrupt();
        while (_broadcasterThread.isAlive()) {
            try {
                _broadcasterThread.join();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void broadcast() {
        try {
            while (!Thread.interrupted() && !_socket.isClosed()) {
                Thread.sleep(broadcastInterval);

                try {
                    var sendData = PeerDiscoveryInfo.newBuilder()
                            .setUuid(persistentRemoteHostsService.getSelfUuid().toString())
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
                            } catch (Exception ignored) {
                                continue;
                            }

//                            Log.trace(getClass().getName() + "Broadcast sent to: " + broadcast.getHostAddress()
//                                    + ", at: " + networkInterface.getDisplayName());
                        }
                    }

                } catch (Exception ignored) {
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("PeerDiscoveryServer stopped");
    }
}
