package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OpSender {
    @Inject
    PeerManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    private static final int _threads = 1;
    private ExecutorService _executor;
    private volatile boolean _shutdown = false;

    private final HashSetDelayedBlockingQueue<OpObject<?>> _queue = new HashSetDelayedBlockingQueue<>(0); // FIXME:

    @Startup
    void init() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("opsender-%d")
                .build();

        _executor = Executors.newFixedThreadPool(_threads, factory);

        for (int i = 0; i < _threads; i++) {
            _executor.submit(this::sender);
        }
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) throws InterruptedException {
        _shutdown = true;
        _executor.shutdownNow();
        if (!_executor.awaitTermination(30, TimeUnit.SECONDS)) {
            Log.error("Failed to shut down op sender thread");
        }
    }

    private void sender() {
        while (!_shutdown) {
            try {
                var got = _queue.get();
                for (var h : remoteHostManager.getAvailableHosts()) {
                    sendForHost(got, h);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    private <OpLocal extends Op> void sendForHost(OpObject<OpLocal> queue, UUID host) {
        OpLocal op;
        while ((op = queue.getPendingOpForHost(host)) != null) {
            try {
                remoteObjectServiceClient.pushOp(op, queue.getId(), host);
                queue.commitOpForHost(host, op);
            } catch (Exception e) {
                Log.info("Error sending op to " + host, e);
                break;
            }
        }
    }

    public void push(OpObject<?> queue) {
        _queue.readd(queue);
    }
}
