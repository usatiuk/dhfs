package com.usatiuk.dhfs.peerdiscovery.local;

import com.google.protobuf.InvalidProtocolBufferException;
import com.usatiuk.dhfs.peerdiscovery.IpPeerAddress;
import com.usatiuk.dhfs.peerdiscovery.PeerAddressType;
import com.usatiuk.dhfs.peerdiscovery.PeerDiscoveryDirectory;
import com.usatiuk.dhfs.peerdiscovery.PeerDiscoveryInfo;
import com.usatiuk.dhfs.peersync.PeerId;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.*;
import java.nio.ByteBuffer;

@ApplicationScoped
@IfBuildProperty(name = "dhfs.local-discovery", stringValue = "true")
public class LocalPeerDiscoveryClient {
    @Inject
    PeerDiscoveryDirectory peerDiscoveryDirectory;
    @ConfigProperty(name = "dhfs.objects.peerdiscovery.broadcast")
    boolean enabled;
    private Thread _clientThread;
    private DatagramSocket _socket;

    @Startup
    void init() throws SocketException, UnknownHostException {
        if (!enabled) {
            return;
        }
        _socket = new DatagramSocket(42069, InetAddress.getByName("0.0.0.0"));
        _socket.setBroadcast(true);

        _clientThread = new Thread(this::client);
        _clientThread.setName("LocalPeerDiscoveryClient");
        _clientThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        if (!enabled) {
            return;
        }
        _socket.close();
        _clientThread.interrupt();
        _clientThread.interrupt();
        while (_clientThread.isAlive()) {
            try {
                _clientThread.join();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void client() {
        while (!Thread.interrupted() && !_socket.isClosed()) {
            try {
                byte[] buf = new byte[10000];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                _socket.receive(packet);

                try {
                    var got = PeerDiscoveryInfo.parseFrom(ByteBuffer.wrap(buf, 0, packet.getLength()));
                    Log.tracev("Got peer discovery packet from {0}", packet.getAddress());
                    peerDiscoveryDirectory.notifyAddr(
                            new IpPeerAddress(
                                    PeerId.of(got.getUuid()),
                                    PeerAddressType.LAN,
                                    packet.getAddress(),
                                    got.getPort(),
                                    got.getSecurePort()
                            )
                    );
                } catch (InvalidProtocolBufferException e) {
                    continue;
                }
            } catch (Exception ex) {
                Log.error(ex);
            }
        }
        Log.info("PeerDiscoveryClient stopped");
    }
}
