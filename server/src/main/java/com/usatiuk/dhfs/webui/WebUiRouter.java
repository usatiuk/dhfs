package com.usatiuk.dhfs.webui;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@ApplicationScoped
public class WebUiRouter {

    @ConfigProperty(name = "dhfs.webui.root")
    Optional<String> root;

    void installRoute(@Observes StartupEvent startupEvent, Router router) {
        root.ifPresent(r -> {
            router.route()
                    .path("/webui/*")
                    .handler(this::handle);
        });
    }

    public void handle(RoutingContext event) {
        var indexHtml = Paths.get(root.orElseThrow(() -> new IllegalStateException("Web ui root not set but handler called")), "index.html").toString();

        HttpServerRequest request = event.request();
        String requestedPath = Path.of(event.currentRoute().getPath()).relativize(Path.of(event.normalizedPath())).toString();

        if ("/".equals(requestedPath)) {
            request.response().sendFile(indexHtml);
            return;
        }

        Path requested = Paths.get(root.get(), requestedPath);
        if (!requested.normalize().startsWith(Paths.get(root.get()))) {
            request.response().setStatusCode(404).end();
            return;
        }

        event.vertx().fileSystem().lprops(requested.toString(), exists -> {
            if (exists.succeeded() && exists.result().isRegularFile())
                request.response().sendFile(requested.toString());
            else
                request.response().sendFile(indexHtml);
        });
    }
}
