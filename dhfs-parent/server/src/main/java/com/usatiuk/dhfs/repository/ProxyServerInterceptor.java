package com.usatiuk.dhfs.repository;

import io.grpc.*;
import jakarta.enterprise.context.ApplicationScoped;

import static com.usatiuk.dhfs.repository.ProxyConstants.*;

@ApplicationScoped
public class ProxyServerInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall,
                                                                 Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context context = null;
        if (metadata.containsKey(PROXY_TO_HEADER_KEY)
                && metadata.containsKey(PROXY_FROM_HEADER_KEY)) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Both proxy_to and proxy_from headers are present"));
        }

        if (metadata.containsKey(PROXY_TO_HEADER_KEY)) {
            var socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            context = Context.current().withValue(PROXY_TO_HEADER_KEY_CTX, metadata.get(PROXY_TO_HEADER_KEY))
                    .withValue(PROXY_TO_FROM_ADDR_KEY_CTX, socketAddress);
        } else if (metadata.containsKey(PROXY_FROM_HEADER_KEY)) {
            context = Context.current().withValue(PROXY_FROM_HEADER_KEY_CTX, metadata.get(PROXY_FROM_HEADER_KEY));
        }


        if (context != null) {
            return Contexts.interceptCall(
                    context,
                    serverCall,
                    metadata,
                    serverCallHandler
            );
        } else {
            return serverCallHandler.startCall(serverCall, metadata);
        }
    }
}
