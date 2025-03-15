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

    private final Optional<String> _root;

    public WebUiRouter(@ConfigProperty(name = "dhfs.webui.root") Optional<String> root) {
        _root = root.map(s -> Path.of(s).normalize().toString());
    }

    void installRoute(@Observes StartupEvent startupEvent, Router router) {
        _root.ifPresent(r -> {
            router.route().path("/").handler(ctx -> ctx.redirect("/webui"));
            router.route()
                    .path("/webui/*")
                    .handler(this::handle);
        });
    }

    public void handle(RoutingContext event) {
        var indexHtml = Paths.get(_root.orElseThrow(() -> new IllegalStateException("Web ui root not set but handler called")), "index.html").toString();

        HttpServerRequest request = event.request();
        String requestedPath = Path.of(event.currentRoute().getPath()).relativize(Path.of(event.normalizedPath())).toString();

        if ("/".equals(requestedPath)) {
            request.response().sendFile(indexHtml);
            return;
        }

        Path requested = Paths.get(_root.get(), requestedPath);
        if (!requested.normalize().startsWith(Paths.get(_root.get()))) {
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
