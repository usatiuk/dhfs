package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.repository.RemoteHostManager;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OpSender {
    @Inject
    RemoteHostManager remoteHostManager;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;

    private static final int _threads = 1;
    private ExecutorService _executor;
    private volatile boolean _shutdown = false;

    private final HashSetDelayedBlockingQueue<OpQueue> _queue = new HashSetDelayedBlockingQueue<>(0); // FIXME:

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
                    Op op;
                    while ((op = got.getForHost(h)) != null) {
                        try {
                            remoteObjectServiceClient.pushOp(op, got.getId(), h);
                            got.commitOneForHost(h, op);
                        } catch (Exception e) {
                            Log.info("Error sending op to " + h, e);
                            break;
                        }
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void push(OpQueue queue) {
        _queue.readd(queue);
    }
}
