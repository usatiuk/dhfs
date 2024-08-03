package com.usatiuk.dhfs.objects.protoserializer;

import com.google.protobuf.Message;
import com.usatiuk.dhfs.objects.persistence.*;
import io.quarkus.arc.ClientProxy;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

@ApplicationScoped
public class ProtoSerializerService {

    @FunctionalInterface
    public interface SerializationFn<M extends Message, O> {
        M apply(O obj);
    }

    @FunctionalInterface
    public interface DeserializationFn<M extends Message, O> {
        O apply(M message);
    }

    private final HashMap<Class<?>, SerializationFn<? extends Message, ?>> _serializers = new HashMap<>();
    private final HashMap<Class<? extends Message>, DeserializationFn<? extends Message, ?>> _deserializers = new HashMap<>();

    // Needed as otherwise they are removed
    @Inject
    Instance<ProtoSerializer<?, ?>> _protoSerializers;

    @Inject
    Instance<ProtoDeserializer<?, ?>> _protoDeserializers;

    @PostConstruct
    void init() {
        for (var s : _protoSerializers) {
            var args = ((ParameterizedType) Arrays.stream(ClientProxy.unwrap(s).getClass().getGenericInterfaces())
                                                  .filter(t -> {
                                                      if (t instanceof ParameterizedType)
                                                          return ((ParameterizedType) t).getRawType().equals(ProtoSerializer.class);
                                                      return false;
                                                  }).findFirst().orElseThrow(() -> new IllegalArgumentException("ProtoSerializer interface not found on ProtoSerializer?")))
                    .getActualTypeArguments(); //FIXME:
            Class<? extends Message> messageClass = (Class<? extends Message>) args[0];
            Class<?> objClass = (Class<?>) args[1];

            if (_serializers.containsKey(objClass))
                throw new IllegalStateException("Already registered serializer for: " + objClass);

            _serializers.put(objClass, obj -> ((ProtoSerializer) s).serialize(obj));
        }

        for (var s : _protoDeserializers) {
            var args = ((ParameterizedType) Arrays.stream(ClientProxy.unwrap(s).getClass().getGenericInterfaces())
                                                  .filter(t -> {
                                                      if (t instanceof ParameterizedType)
                                                          return ((ParameterizedType) t).getRawType().equals(ProtoDeserializer.class);
                                                      return false;
                                                  }).findFirst().orElseThrow(() -> new IllegalArgumentException("ProtoSerializer interface not found on ProtoSerializer?")))
                    .getActualTypeArguments(); //FIXME:
            Class<? extends Message> messageClass = (Class<? extends Message>) args[0];
            Class<?> objClass = (Class<?>) args[1];

            if (_deserializers.containsKey(messageClass))
                throw new IllegalStateException("Already registered deserializer: " + messageClass);

            _deserializers.put(messageClass, msg -> ((ProtoDeserializer) s).deserialize(msg));
        }
    }

    public <M extends Message, O> M serialize(O object) {
        if (!_serializers.containsKey(object.getClass()))
            throw new IllegalStateException("Serializer not registered: " + object.getClass());
        return ((SerializationFn<M, O>) _serializers.get(object.getClass())).apply(object);
    }

    // FIXME: This is annoying
    private <O> Optional<JObjectDataP> serializeToJObjectDataPInternal(O object) {
        var ser = serialize(object);
        if (ser instanceof FileP) {
            return Optional.of(JObjectDataP.newBuilder().setFile((FileP) ser).build());
        } else if (ser instanceof DirectoryP) {
            return Optional.of(JObjectDataP.newBuilder().setDirectory((DirectoryP) ser).build());
        } else if (ser instanceof ChunkInfoP) {
            return Optional.of(JObjectDataP.newBuilder().setChunkInfo((ChunkInfoP) ser).build());
        } else if (ser instanceof ChunkDataP) {
            return Optional.of(JObjectDataP.newBuilder().setChunkData((ChunkDataP) ser).build());
        } else if (ser instanceof PeerDirectoryP) {
            return Optional.of(JObjectDataP.newBuilder().setPeerDirectory((PeerDirectoryP) ser).build());
        } else if (ser instanceof PersistentPeerInfoP) {
            return Optional.of(JObjectDataP.newBuilder().setPersistentPeerInfo((PersistentPeerInfoP) ser).build());
        } else if (ser instanceof TreeNodeP) {
            return Optional.of(JObjectDataP.newBuilder().setTreeNode((TreeNodeP) ser).build());
        } else {
            return Optional.empty();
        }
    }

    // FIXME: This is annoying
    public <O> JObjectDataP serializeToJObjectDataP(O object) {
        if (object == null) throw new IllegalArgumentException("Object to serialize shouldn't be null");

        return serializeToJObjectDataPInternal(object).orElseThrow(() -> new IllegalStateException("Unknown JObjectDataP type: " + object.getClass()));
    }

    public <M extends Message, O> O deserialize(M message) {
        if (!_deserializers.containsKey(message.getClass()))
            throw new IllegalStateException("Deserializer not registered: " + message.getClass());
        return ((DeserializationFn<M, O>) _deserializers.get(message.getClass())).apply(message);
    }
}
