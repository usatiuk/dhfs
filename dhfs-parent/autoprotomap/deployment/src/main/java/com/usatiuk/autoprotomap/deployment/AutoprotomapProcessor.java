package com.usatiuk.autoprotomap.deployment;

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
import org.jboss.jandex.ClassType;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Type;
import org.objectweb.asm.Opcodes;

class AutoprotomapProcessor {

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

    public static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
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
            var consideredFieldName = f.name();

            if (consideredFieldName.startsWith("_")) // TODO: Configure
                consideredFieldName = consideredFieldName.substring(1);

            switch (f.type().kind()) {
                case CLASS -> {
                    if (f.type().equals(Type.create(String.class))) {
                        //TODO:String
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
                    var setter = MethodDescriptor.ofMethod(builderType.name().toString(), "set" + capitalize(consideredFieldName),
                            builderType.name().toString(), f.type().toString());

                    var val = bytecodeCreator.readInstanceField(f, object);
                    bytecodeCreator.invokeVirtualMethod(setter, builder, val);
                }
                case WILDCARD_TYPE -> throw new UnsupportedOperationException("Wildcards not supported yet");
                case PARAMETERIZED_TYPE ->
                        throw new UnsupportedOperationException("Parametrized types not supported yet");
                case ARRAY -> throw new UnsupportedOperationException("Arrays not supported yet");
                default -> throw new IllegalStateException("Unexpected type: " + f.type());
            }
        }
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
