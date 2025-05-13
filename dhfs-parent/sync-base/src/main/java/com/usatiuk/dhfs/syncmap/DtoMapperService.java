package com.usatiuk.dhfs.syncmap;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Singleton;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Service for mapping between remote objects and their DTO representations.
 */
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

    /**
     * Converts a remote object to its DTO representation.
     *
     * @param from the remote object to convert
     * @param to   the class of the DTO representation
     * @param <F>  the type of the remote object
     * @param <D>  the type of the DTO
     * @return the DTO representation of the remote object
     */
    public <F extends JDataRemote, D extends JDataRemoteDto> D toDto(F from, Class<D> to) {
        if (to.equals(from.getClass())) {
            return (D) from;
        }
        var got = _remoteToDtoMap.get(from.getClass()).toDto(from);
        assert to.isInstance(got);
        return to.cast(got);
    }

    /**
     * Converts a DTO to its corresponding remote object.
     *
     * @param from the DTO to convert
     * @param to   the class of the remote object representation
     * @param <F>  the type of the remote object
     * @param <D>  the type of the DTO
     * @return the remote object representation of the DTO
     */
    public <F extends JDataRemote, D extends JDataRemoteDto> F fromDto(D from, Class<F> to) {
        if (to.equals(from.getClass())) {
            return (F) from;
        }
        var got = _dtoToRemoteMap.get(from.getClass()).fromDto(from);
        assert to.isInstance(got);
        return to.cast(got);
    }
}
