package com.usatiuk.autoprotomap.deployment;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.quarkus.gizmo.*;
import org.jboss.jandex.Type;
import org.jboss.jandex.*;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.usatiuk.autoprotomap.deployment.Constants.*;

public class ProtoSerializerGenerator {
    private final Index index;
    private final ProtoIndexBuildItem protoIndex;
    private final ClassCreator classCreator;
    private final Type topMessageType;
    private final Type topObjectType;

    public ProtoSerializerGenerator(Index index, ProtoIndexBuildItem protoIndex, ClassCreator classCreator, Type topMessageType, Type topObjectType) {
        this.index = index;
        this.protoIndex = protoIndex;
        this.classCreator = classCreator;
        this.topMessageType = topMessageType;
        this.topObjectType = topObjectType;
    }

    private void traverseHierarchy(Index index, ClassInfo klass, Consumer<ClassInfo> visitor) {
        var cur = klass;
        while (true) {
            visitor.accept(cur);

            var next = cur.superClassType().name();
            if (next.equals(DotName.OBJECT_NAME)) break;
            cur = index.getClassByName(next);
        }
    }

    private ArrayList<FieldInfo> findAllFields(Index index, ClassInfo klass) {
        ArrayList<FieldInfo> ret = new ArrayList<>();
        traverseHierarchy(index, klass, cur -> {
            ret.addAll(cur.fields());
        });
        return ret;
    }

    private void generateBuilderUse(BytecodeCreator bytecodeCreator,
                                    ResultHandle builder,
                                    Type messageType, Type objectType,
                                    ResultHandle object) {
        var builderType = Type.create(DotName.createComponentized(messageType.name(), "Builder", true), Type.Kind.CLASS);

        var objectClass = index.getClassByName(objectType.name().toString());

        for (var f : findAllFields(index, objectClass)) {
            var consideredFieldName = stripPrefix(f.name(), FIELD_PREFIX);

            Supplier<ResultHandle> get = () -> {
                if ((f.flags() & Opcodes.ACC_PUBLIC) != 0)
                    return bytecodeCreator.readInstanceField(f, object);
                else {
                    var fieldGetter = "get" + capitalize(stripPrefix(f.name(), FIELD_PREFIX));
                    return bytecodeCreator.invokeVirtualMethod(
                            MethodDescriptor.ofMethod(objectType.toString(), fieldGetter, f.type().name().toString()), object);
                }
            };

            Effect doSimpleCopy = () -> {
                var setter = MethodDescriptor.ofMethod(builderType.name().toString(), "set" + capitalize(consideredFieldName),
                        builderType.name().toString(), f.type().toString());

                var val = get.get();
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

                        var val = get.get();

                        generateBuilderUse(bytecodeCreator, nestedBuilder, Type.create(protoType.name(), Type.Kind.CLASS), f.type(), val);
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
            BytecodeCreator bytecodeCreator,
            ClassCreator classCreator,
            Type messageType, Type objectType,
            ResultHandle message
    ) {
        var constructor = findAllArgsConstructor(index, index.getClassByName(objectType.name()));
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
                        argMap[i] = generateConstructorUse(bytecodeCreator, classCreator, Type.create(nestedProtoType.name(), Type.Kind.CLASS), type, nested);
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

    private MethodInfo findAllArgsConstructor(Index index, ClassInfo klass) {
        ArrayList<FieldInfo> fields = findAllFields(index, klass);

        var fieldCount = fields.size();
        var fieldNames = fields.stream().map(f -> stripPrefix(f.name(), FIELD_PREFIX)).sorted().toList();
        var fieldNameToType = fields.stream().collect(Collectors.toMap(f -> stripPrefix(f.name(), FIELD_PREFIX), FieldInfo::type));

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

    public void generateAbstract() {
        var kids = index.getAllKnownSubclasses(topObjectType.name()).stream()
                .filter(k -> !k.isAbstract()).toList();

        try (MethodCreator method = classCreator.getMethodCreator("serialize",
                Message.class, Object.class)) {

            method.setModifiers(Opcodes.ACC_PUBLIC);

            var builderType = Type.create(DotName.createComponentized(topMessageType.name(), "Builder", true), Type.Kind.CLASS);

            var builder = method.invokeStaticMethod(MethodDescriptor.ofMethod(topMessageType.name().toString(), "newBuilder", builderType.name().toString()));

            var arg = method.getMethodParam(0);

            for (var nestedObjClass : kids) {
                System.out.println("Generating " + nestedObjClass.name() + " serializer for " + topObjectType.name());
                var nestedMessageClass = protoIndex.protoMsgToObj.inverseBidiMap().get(nestedObjClass);
                var nestedMessageType = Type.create(nestedMessageClass.name(), Type.Kind.CLASS);

                var nestedObjType = Type.create(nestedObjClass.name(), Type.Kind.CLASS);

                var statement = method.ifTrue(method.instanceOf(arg, nestedObjClass.name().toString()));

                try (var branch = statement.trueBranch()) {
                    var nestedBuilderType = Type.create(DotName.createComponentized(nestedMessageType.name(), "Builder", true), Type.Kind.CLASS);
                    var nestedBuilder = branch.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(),
                            "get" + capitalize(nestedObjType.name().withoutPackagePrefix()) + "Builder",
                            nestedBuilderType.name().toString()), builder);
                    generateBuilderUse(branch, nestedBuilder, nestedMessageType, nestedObjType, arg);
                    var result = branch.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(), "build", topMessageType.name().toString()), builder);
                    branch.returnValue(result);
                }
            }
            method.throwException(IllegalArgumentException.class, "Unknown object type");
        }

        try (MethodCreator method = classCreator.getMethodCreator("deserialize",
                Object.class, Message.class)) {
            method.setModifiers(Opcodes.ACC_PUBLIC);
            var arg = method.getMethodParam(0);

            for (var nestedObjClass : kids) {
                System.out.println("Generating " + nestedObjClass.name() + " deserializer for " + topObjectType.name());
                var nestedMessageClass = protoIndex.protoMsgToObj.inverseBidiMap().get(nestedObjClass);
                var nestedMessageType = Type.create(nestedMessageClass.name(), Type.Kind.CLASS);

                var nestedObjType = Type.create(nestedObjClass.name(), Type.Kind.CLASS);
                var typeCheck = method.invokeVirtualMethod(MethodDescriptor.ofMethod(topMessageType.name().toString(),
                        "has" + capitalize(nestedObjType.name().withoutPackagePrefix()), boolean.class), arg);

                var statement = method.ifTrue(typeCheck);

                try (var branch = statement.trueBranch()) {
                    var nestedMessage = branch.invokeVirtualMethod(MethodDescriptor.ofMethod(topMessageType.name().toString(),
                            "get" + capitalize(nestedObjType.name().withoutPackagePrefix()), nestedMessageType.name().toString()), arg);
                    branch.returnValue(generateConstructorUse(branch, classCreator, nestedMessageType, nestedObjType, nestedMessage));
                }
            }
            method.throwException(IllegalArgumentException.class, "Unknown object type");
        }
    }

    public void generate() {
        var objInfo = index.getClassByName(topObjectType.name());
        if (objInfo.isAbstract()) {
            generateAbstract();
            return;
        }

        try (MethodCreator method = classCreator.getMethodCreator("serialize",
                Message.class, Object.class)) {

            method.setModifiers(Opcodes.ACC_PUBLIC);

            var builderType = Type.create(DotName.createComponentized(topMessageType.name(), "Builder", true), Type.Kind.CLASS);

            var builder = method.invokeStaticMethod(MethodDescriptor.ofMethod(topMessageType.name().toString(), "newBuilder", builderType.name().toString()));

            var arg = method.getMethodParam(0);

            generateBuilderUse(method, builder, topMessageType, topObjectType, arg);

            var result = method.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(), "build", topMessageType.name().toString()), builder);

            method.returnValue(result);
        }

        try (MethodCreator method = classCreator.getMethodCreator("deserialize",
                Object.class, Message.class)) {
            method.setModifiers(Opcodes.ACC_PUBLIC);

            var arg = method.getMethodParam(0);

            method.returnValue(generateConstructorUse(method, classCreator, topMessageType, topObjectType, arg));
        }

    }
}
