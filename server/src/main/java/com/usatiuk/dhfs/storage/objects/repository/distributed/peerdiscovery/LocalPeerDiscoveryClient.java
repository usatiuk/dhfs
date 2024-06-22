package com.usatiuk.dhfs.storage.objects.repository.distributed.peerdiscovery;

import com.google.protobuf.InvalidProtocolBufferException;
import com.usatiuk.dhfs.objects.repository.distributed.peerdiscovery.PeerDiscoveryInfo;
import com.usatiuk.dhfs.storage.objects.repository.distributed.RemoteHostManager;
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
        _clientThread.interrupt();
        _clientThread.join();
    }

    private void client() {
        while (!Thread.interrupted()) {
            try {
                byte[] buf = new byte[10000];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                _socket.receive(packet);

                try {
                    var got = PeerDiscoveryInfo.parseFrom(ByteBuffer.wrap(buf, 0, packet.getLength()));

                    remoteHostManager.notifyAddr(UUID.fromString(got.getUuid()), packet.getAddress().toString(), got.getPort());

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
