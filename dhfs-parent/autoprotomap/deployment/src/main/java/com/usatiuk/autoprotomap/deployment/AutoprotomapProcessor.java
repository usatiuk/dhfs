package com.usatiuk.autoprotomap.deployment;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.gizmo.*;
import jakarta.inject.Singleton;
import org.jboss.jandex.Type;
import org.jboss.jandex.*;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

class AutoprotomapProcessor {
    @FunctionalInterface
    public interface Effect {
        void apply();
    }

    private static final String FIELD_PREFIX = "_";

    private static final String FEATURE = "autoprotomap";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    ProtoIndexBuildItem index(ApplicationIndexBuildItem jandex) {
        var ret = new ProtoIndexBuildItem();
        var annot = jandex.getIndex().getAnnotations(ProtoMirror.class);
        for (var a : annot) {
            var protoTarget = jandex.getIndex().getClassByName(((ClassType) a.value().value()).name());
//            if (!messageImplementors.contains(protoTarget))
//                throw new IllegalArgumentException("Expected " + protoTarget + " to be a proto message");
            System.out.println("Found: " + a.name().toString() + " at " + protoTarget.name().toString() + " of " + a.target().asClass().name().toString());
            ret.protoMsgToObj.put(protoTarget, a.target().asClass());
        }
        return ret;
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private static String stripPrefix(String str, String prefix) {
        if (str.startsWith(prefix)) {
            return str.substring(prefix.length());
        }
        return str;
    }

    private void generateBuilderUse(Index index,
                                    ProtoIndexBuildItem protoIndex,
                                    BytecodeCreator bytecodeCreator,
                                    ClassCreator classCreator,
                                    ResultHandle builder,
                                    Type messageType, Type objectType,
                                    ResultHandle object) {
        var builderType = Type.create(DotName.createComponentized(messageType.name(), "Builder", true), Type.Kind.CLASS);

        var objectClass = index.getClassByName(objectType.name().toString());

        for (var f : objectClass.fields()) {
            var consideredFieldName = stripPrefix(f.name(), FIELD_PREFIX);

            Effect doSimpleCopy = () -> {
                var setter = MethodDescriptor.ofMethod(builderType.name().toString(), "set" + capitalize(consideredFieldName),
                        builderType.name().toString(), f.type().toString());

                var val = bytecodeCreator.readInstanceField(f, object);
                bytecodeCreator.invokeVirtualMethod(setter, builder, val);
            };

            switch (f.type().kind()) {
                case CLASS -> {
                    if (f.type().equals(Type.create(String.class)) || f.type().equals(Type.create(ByteString.class))) {
                        doSimpleCopy.apply();
                    } else {
                        var builderGetter = "get" + capitalize(f.name()) + "Builder";
                        var protoType = protoIndex.protoMsgToObj.inverseBidiMap().get(index.getClassByName(f.type().name()));
                        var nestedBuilderType = Type.create(DotName.createComponentized(protoType.name(), "Builder", true), Type.Kind.CLASS);
                        var nestedBuilder = bytecodeCreator.invokeVirtualMethod(
                                MethodDescriptor.ofMethod(builderType.toString(), builderGetter, nestedBuilderType.name().toString()), builder);

                        var val = bytecodeCreator.readInstanceField(f, object);

                        generateBuilderUse(index, protoIndex, bytecodeCreator, classCreator, nestedBuilder, Type.create(protoType.name(), Type.Kind.CLASS), f.type(), val);
                    }
                }
                case PRIMITIVE -> {
                    doSimpleCopy.apply();
                }
                case WILDCARD_TYPE -> throw new UnsupportedOperationException("Wildcards not supported yet");
                case PARAMETERIZED_TYPE ->
                        throw new UnsupportedOperationException("Parametrized types not supported yet");
                case ARRAY -> throw new UnsupportedOperationException("Arrays not supported yet");
                default -> throw new IllegalStateException("Unexpected type: " + f.type());
            }
        }
    }

    private ResultHandle generateConstructorUse(
            Index index,
            ProtoIndexBuildItem protoIndex,
            BytecodeCreator bytecodeCreator,
            ClassCreator classCreator,
            Type messageType, Type objectType,
            ResultHandle message
    ) {
        var constructor = findAllArgsConstructor(index.getClassByName(objectType.name()));
        if (constructor == null) {
            throw new IllegalStateException("No constructor found for type: " + objectType.name());
        }
        var argMap = new ResultHandle[constructor.parametersCount()];

        for (int i = 0; i < argMap.length; i++) {
            var type = constructor.parameterType(i);
            var strippedName = stripPrefix(constructor.parameterName(i), FIELD_PREFIX);

            IntConsumer doSimpleCopy = (arg) -> {
                var call = MethodDescriptor.ofMethod(messageType.name().toString(), "get" + capitalize(strippedName),
                        type.name().toString());
                argMap[arg] = bytecodeCreator.invokeVirtualMethod(call, message);
            };

            switch (type.kind()) {
                case CLASS -> {
                    if (type.equals(Type.create(String.class)) || type.equals(Type.create(ByteString.class))) {
                        doSimpleCopy.accept(i);
                    } else {
                        var nestedProtoType = protoIndex.protoMsgToObj.inverseBidiMap().get(index.getClassByName(type.name()));
                        var call = MethodDescriptor.ofMethod(messageType.name().toString(), "get" + capitalize(strippedName),
                                nestedProtoType.name().toString());
                        var nested = bytecodeCreator.invokeVirtualMethod(call, message);
                        argMap[i] = generateConstructorUse(index, protoIndex, bytecodeCreator, classCreator, Type.create(nestedProtoType.name(), Type.Kind.CLASS), type, nested);
                    }
                }
                case PRIMITIVE -> {
                    doSimpleCopy.accept(i);
                }
                case WILDCARD_TYPE -> throw new UnsupportedOperationException("Wildcards not supported yet");
                case PARAMETERIZED_TYPE ->
                        throw new UnsupportedOperationException("Parametrized types not supported yet");
                case ARRAY -> throw new UnsupportedOperationException("Arrays not supported yet");
                default -> throw new IllegalStateException("Unexpected type: " + type);
            }
        }

        return bytecodeCreator.newInstance(constructor, argMap);
    }

    private MethodInfo findAllArgsConstructor(ClassInfo klass) {
        var fieldCount = klass.fields().size();
        var fieldNames = klass.fields().stream().map(f -> stripPrefix(f.name(), FIELD_PREFIX)).sorted().toList();
        var fieldNameToType = klass.fields().stream().collect(Collectors.toMap(f -> stripPrefix(f.name(), FIELD_PREFIX), FieldInfo::type));

        for (var m : klass.constructors()) {
            if (m.parametersCount() != fieldCount) continue;
            var parameterNames = m.parameters().stream().map(n -> stripPrefix(n.name(), FIELD_PREFIX)).sorted().toList();
            if (!Objects.equals(fieldNames, parameterNames)) continue;

            for (var p : m.parameters()) {
                if (!Objects.equals(fieldNameToType.get(stripPrefix(p.name(), FIELD_PREFIX)), p.type())) continue;
            }

            return m;
        }

        return null;
    }

    @BuildStep
    void generateProtoSerializer(ApplicationIndexBuildItem jandex, ProtoIndexBuildItem protoIndex, BuildProducer<GeneratedBeanBuildItem> generatedClasses) {
        try {
            for (var o : protoIndex.protoMsgToObj.entrySet()) {
                System.out.println("Generating " + o.getKey().toString() + " -> " + o.getValue().toString());
                var gizmoAdapter = new GeneratedBeanGizmoAdaptor(generatedClasses);

                var msgType = io.quarkus.gizmo.Type.classType(o.getKey().name());
                var objType = io.quarkus.gizmo.Type.classType(o.getValue().name());

                var type = io.quarkus.gizmo.Type.ParameterizedType.parameterizedType(
                        io.quarkus.gizmo.Type.classType(ProtoSerializer.class),
                        msgType, objType);

                var msgJType = Type.create(o.getKey().name(), Type.Kind.CLASS);
                var objJType = Type.create(o.getValue().name(), Type.Kind.CLASS);

                try (ClassCreator classCreator = ClassCreator.builder()
                        .className("com.usatiuk.autoprotomap.generated.for" + o.getKey().simpleName())
                        .signature(SignatureBuilder.forClass().addInterface(type))
                        .classOutput(gizmoAdapter)
                        .setFinal(true)
                        .build()) {
                    classCreator.addAnnotation(Singleton.class);

                    try (MethodCreator method = classCreator.getMethodCreator("serialize",
                            Message.class, Object.class)) {

                        method.setModifiers(Opcodes.ACC_PUBLIC);

                        var builderType = Type.create(DotName.createComponentized(msgJType.name(), "Builder", true), Type.Kind.CLASS);

                        var builder = method.invokeStaticMethod(MethodDescriptor.ofMethod(msgJType.name().toString(), "newBuilder", builderType.name().toString()));

                        var arg = method.getMethodParam(0);

                        generateBuilderUse(jandex.getIndex(), protoIndex, method, classCreator, builder, msgJType, objJType, arg);

                        var result = method.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(), "build", msgJType.name().toString()), builder);

                        method.returnValue(result);
                    }

                    try (MethodCreator method = classCreator.getMethodCreator("deserialize",
                            Object.class, Message.class)) {
                        method.setModifiers(Opcodes.ACC_PUBLIC);

                        var objClassInfo = jandex.getIndex().getClassByName(objJType.name());

                        var arg = method.getMethodParam(0);

                        method.returnValue(
                                generateConstructorUse(jandex.getIndex(), protoIndex, method, classCreator, msgJType, objJType, arg)
                        );
                    }
                }
            }
        } catch (Throwable e) {
            StringBuilder sb = new StringBuilder();
            sb.append(e.toString() + "\n");
            for (var el : e.getStackTrace()) {
                sb.append(el.toString() + "\n");
            }
            System.out.println(sb.toString());
        }
    }
}
