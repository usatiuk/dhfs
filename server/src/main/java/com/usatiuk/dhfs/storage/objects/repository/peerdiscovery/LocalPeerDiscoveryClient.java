package com.usatiuk.dhfs.storage.objects.repository.peerdiscovery;

import com.google.protobuf.InvalidProtocolBufferException;
import com.usatiuk.dhfs.objects.repository.peerdiscovery.PeerDiscoveryInfo;
import com.usatiuk.dhfs.storage.objects.repository.RemoteHostManager;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.UUID;

@ApplicationScoped
public class LocalPeerDiscoveryClient {

    @Inject
    RemoteHostManager remoteHostManager;

    private Thread _clientThread;

    private DatagramSocket _socket;

    @Startup
    void init() throws SocketException, UnknownHostException {
        _socket = new DatagramSocket(42069, InetAddress.getByName("0.0.0.0"));
        _socket.setBroadcast(true);

        _clientThread = new Thread(this::client);
        _clientThread.setName("LocalPeerDiscoveryClient");
        _clientThread.start();
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
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

                    remoteHostManager.notifyAddr(UUID.fromString(got.getUuid()), packet.getAddress().getHostAddress(), got.getPort(), got.getSecurePort());

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
