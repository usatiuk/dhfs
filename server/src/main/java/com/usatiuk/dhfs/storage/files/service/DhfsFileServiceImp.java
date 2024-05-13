package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DhfsFileServiceImp {
    @Inject
    Vertx vertx;
    @Inject
    ObjectRepository objectRepository;
}
