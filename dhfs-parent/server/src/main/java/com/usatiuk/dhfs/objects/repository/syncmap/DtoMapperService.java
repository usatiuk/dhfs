package com.usatiuk.dhfs.objects.repository.syncmap;

import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Singleton;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Singleton
public class DtoMapperService {
    private final Map<Class<? extends JDataRemote>, DtoMapper> _remoteToDtoMap;
    private final Map<Class<? extends JDataRemoteDto>, DtoMapper> _dtoToRemoteMap;

    public DtoMapperService(Instance<DtoMapper<?, ?>> dtoMappers) {
        HashMap<Class<? extends JDataRemote>, DtoMapper> remoteToDtoMap = new HashMap<>();
        HashMap<Class<? extends JDataRemoteDto>, DtoMapper> dtoToRemoteMap = new HashMap<>();

        for (var dtoMapper : dtoMappers.handles()) {
            for (var type : Arrays.stream(dtoMapper.getBean().getBeanClass().getGenericInterfaces()).flatMap(
                    t -> {
                        if (!(t instanceof ParameterizedType pm)) return Stream.empty();
                        if (pm.getRawType().equals(DtoMapper.class)) return Stream.of(pm);
                        return Stream.empty();
                    }
            ).toList()) {
                var orig = type.getActualTypeArguments()[0];
                var dto = type.getActualTypeArguments()[1];
                assert JDataRemote.class.isAssignableFrom((Class<?>) orig);
                assert JDataRemoteDto.class.isAssignableFrom((Class<?>) dto);
                remoteToDtoMap.put((Class<? extends JDataRemote>) orig, dtoMapper.get());
                dtoToRemoteMap.put((Class<? extends JDataRemoteDto>) dto, dtoMapper.get());
            }
        }

        _remoteToDtoMap = Map.copyOf(remoteToDtoMap);
        _dtoToRemoteMap = Map.copyOf(dtoToRemoteMap);
    }

    public <F extends JDataRemote, D extends JDataRemoteDto> D toDto(F from, Class<D> to) {
        if (to.equals(from.getClass())) {
            return (D) from;
        }
        var got = _remoteToDtoMap.get(from.getClass()).toDto(from);
        assert to.isInstance(got);
        return to.cast(got);
    }

    public <F extends JDataRemote, D extends JDataRemoteDto> F fromDto(D from, Class<F> to) {
        if (to.equals(from.getClass())) {
            return (F) from;
        }
        var got = _dtoToRemoteMap.get(from.getClass()).fromDto(from);
        assert to.isInstance(got);
        return to.cast(got);
    }
}
