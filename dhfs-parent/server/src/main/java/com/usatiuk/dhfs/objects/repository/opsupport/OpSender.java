package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.dhfs.objects.jrepository.JObjectTxManager;
import com.usatiuk.dhfs.objects.repository.PeerManager;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class OpSender {
    private static final int _threads = 1;
    private final HashSetDelayedBlockingQueue<OpObject> _queue = new HashSetDelayedBlockingQueue<>(0); // FIXME:
    @Inject
    PeerManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    JObjectTxManager jObjectTxManager;
    private ExecutorService _executor;
    private volatile boolean _shutdown = false;

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
            } catch (Throwable ex) {
                Log.error("In op sender: ", ex);
            }
        }
    }

    void sendForHost(OpObject obj, UUID host) {
        // Must be peeked before getPendingOpForHost
        var periodicPushOp = obj.getPeriodicPushOp();

        long sendCount = 0;
        Op op;

        while ((op = obj.getPendingOpForHost(host)) != null) {
            try {
                remoteObjectServiceClient.pushOp(op, obj.getId(), host);
                Op finalOp = op;
                jObjectTxManager.executeTx(() -> {
                    obj.commitOpForHost(host, finalOp);
                });
                sendCount++;
            } catch (Exception e) {
                Log.warn("Error sending op to " + host, e);
                break;
            }
        }

        if (sendCount == 0) {
            if (periodicPushOp == null) return;
            try {
                remoteObjectServiceClient.pushOp(periodicPushOp, obj.getId(), host);
                Log.debug("Sent periodic op update to " + host + "of" + obj.getId());
            } catch (Exception e) {
                Log.warn("Error pushing periodic op for " + host + " of " + obj.getId(), e);
            }
        } else {
            Log.debug("Sent " + sendCount + " op updates to " + host + "of" + obj.getId());
        }
    }

    public void push(OpObject queue) {
        _queue.readd(queue);
    }
}
