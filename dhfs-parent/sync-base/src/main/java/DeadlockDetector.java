import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class DeadlockDetector {
    private final ExecutorService _executor = Executors.newSingleThreadExecutor();

    void init(@Observes @Priority(1) StartupEvent event) {
        _executor.submit(this::run);
    }

    void shutdown(@Observes @Priority(100000) ShutdownEvent event) {
        _executor.shutdownNow();
    }

    private void run() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        try {
            while (!Thread.interrupted()) {
                Thread.sleep(4000);

                long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.

                if (threadIds != null) {
                    ThreadInfo[] infos = bean.getThreadInfo(threadIds, Integer.MAX_VALUE);

                    StringBuilder sb = new StringBuilder();

                    sb.append("Deadlock detected!\n");

                    for (ThreadInfo info : infos) {
                        StackTraceElement[] stack = info.getStackTrace();
                        sb.append(info.getThreadName()).append("\n");
                        sb.append("getLockedMonitors: ").append(Arrays.toString(info.getLockedMonitors())).append("\n");
                        sb.append("getLockedSynchronizers: ").append(Arrays.toString(info.getLockedSynchronizers())).append("\n");
                        sb.append("waiting on: ").append(info.getLockInfo()).append("\n");
                        sb.append("locked by: ").append(info.getLockOwnerName()).append("\n");
                        sb.append("Stack trace:\n");
                        for (var e : stack) {
                            sb.append(e.toString()).append("\n");
                        }
                        sb.append("===");
                    }

                    Log.error(sb);
                }
            }
        } catch (InterruptedException e) {
        }
        Log.info("Deadlock detector thread exiting");
    }
}
