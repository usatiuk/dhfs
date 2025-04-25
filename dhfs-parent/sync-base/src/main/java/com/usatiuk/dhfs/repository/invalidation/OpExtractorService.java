package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.syncmap.DtoMapper;
import com.usatiuk.objects.JData;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@ApplicationScoped
public class OpExtractorService {
    private final Map<Class<? extends JData>, OpExtractor> _opExtractorMap;

    public OpExtractorService(Instance<OpExtractor<?>> opExtractors) {
        HashMap<Class<? extends JData>, OpExtractor> opExtractorMap = new HashMap<>();

        for (var opExtractor : opExtractors.handles()) {
            for (var type : Arrays.stream(opExtractor.getBean().getBeanClass().getGenericInterfaces()).flatMap(
                    t -> {
                        if (!(t instanceof ParameterizedType pm)) return Stream.empty();
                        if (pm.getRawType().equals(OpExtractor.class)) return Stream.of(pm);
                        return Stream.empty();
                    }
            ).toList()) {
                var orig = type.getActualTypeArguments()[0];
                assert JData.class.isAssignableFrom((Class<?>) orig);
                opExtractorMap.put((Class<? extends JData>) orig, opExtractor.get());
            }
        }

        _opExtractorMap = Map.copyOf(opExtractorMap);
    }

    public @Nullable Pair<List<Op>, Runnable> extractOps(JData data, PeerId peerId) {
        var extractor = _opExtractorMap.get(data.getClass());
        if (extractor == null) {
            return null;
        }
        return extractor.extractOps(data, peerId);
    }
}
