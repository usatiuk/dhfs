package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@ApplicationScoped
public class OpHandlerService {
    private final Map<Class<? extends Op>, OpHandler> _opHandlerMap;

    public OpHandlerService(Instance<OpHandler<?>> OpHandlers) {
        HashMap<Class<? extends Op>, OpHandler> OpHandlerMap = new HashMap<>();

        for (var OpHandler : OpHandlers.handles()) {
            for (var type : Arrays.stream(OpHandler.getBean().getBeanClass().getGenericInterfaces()).flatMap(
                    t -> {
                        if (!(t instanceof ParameterizedType pm)) return Stream.empty();
                        if (pm.getRawType().equals(OpHandler.class)) return Stream.of(pm);
                        return Stream.empty();
                    }
            ).toList()) {
                var orig = type.getActualTypeArguments()[0];
                assert Op.class.isAssignableFrom((Class<?>) orig);
                OpHandlerMap.put((Class<? extends Op>) orig, OpHandler.get());
            }
        }

        _opHandlerMap = Map.copyOf(OpHandlerMap);
    }

    public void handleOp(PeerId from, Op op) {
        var handler = _opHandlerMap.get(op.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("No handler for op: " + op.getClass());
        }
        handler.handleOp(from, op);
    }
}
