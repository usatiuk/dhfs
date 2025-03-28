package com.usatiuk.dhfs.repository;

import io.grpc.Context;
import io.grpc.Metadata;

import java.net.SocketAddress;

public abstract class ProxyConstants {
    static final Metadata.Key<String> PROXY_TO_HEADER_KEY = Metadata.Key.of("proxy_to", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> PROXY_FROM_HEADER_KEY = Metadata.Key.of("proxy_from", Metadata.ASCII_STRING_MARSHALLER);

    static final Context.Key<String> PROXY_TO_HEADER_KEY_CTX = Context.key("proxy_to");
    static final Context.Key<SocketAddress> PROXY_TO_FROM_ADDR_KEY_CTX = Context.key("proxy_to_from_addr");
    static final Context.Key<String> PROXY_FROM_HEADER_KEY_CTX = Context.key("proxy_from");
}
