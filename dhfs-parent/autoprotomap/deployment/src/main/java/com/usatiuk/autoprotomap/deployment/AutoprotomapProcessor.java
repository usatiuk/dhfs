package com.usatiuk.autoprotomap.deployment;

import com.google.protobuf.Message;
import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.arc.Unremovable;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.devui.runtime.logstream.LogStreamRecorder;
import io.quarkus.gizmo.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Singleton;
import org.jboss.jandex.*;
import org.jboss.jandex.Type;
import org.objectweb.asm.Opcodes;

import java.util.Arrays;
import java.util.List;

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
            ret.protoMsgToObj.put(protoTarget, a.target().asClass());
        }
        return ret;
    }

    public static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    @BuildStep
    void generateProtoSerializer(ApplicationIndexBuildItem jandex, ProtoIndexBuildItem protoIndex, BuildProducer<GeneratedBeanBuildItem> generatedClasses) {
        try {
            for (var o : protoIndex.protoMsgToObj.entrySet()) {
                var gizmoAdapter = new GeneratedBeanGizmoAdaptor(generatedClasses);

                var msgType = io.quarkus.gizmo.Type.classType(o.getKey().name());
                var objType = io.quarkus.gizmo.Type.classType(o.getValue().name());

                var type = io.quarkus.gizmo.Type.ParameterizedType.parameterizedType(
                        io.quarkus.gizmo.Type.classType(ProtoSerializer.class),
                        msgType, objType);

                var msgJType = Type.create(o.getKey().name(), Type.Kind.CLASS);
                var objJType = Type.create(o.getValue().name(), Type.Kind.CLASS);

                try (ClassCreator classCreator = ClassCreator.builder()
                        .className("com.usatiuk.protoautomap.generated.test1")
                        .signature(SignatureBuilder.forClass().addInterface(type))
                        .classOutput(gizmoAdapter)
                        .build()) {

                    classCreator.addAnnotation(Singleton.class);
                    classCreator.addAnnotation(Unremovable.class);
                    System.out.println("0");

                    try (MethodCreator returnHello = classCreator.getMethodCreator("serialize",
                            Object.class, Object.class)) {
                        returnHello.setModifiers(Opcodes.ACC_PUBLIC);

                        var builderType = Type.create(DotName.createComponentized(msgJType.name(), "Builder", true), Type.Kind.CLASS);

                        System.out.println("1");
                        var builder = returnHello.invokeStaticMethod(MethodDescriptor.ofMethod(msgJType.name().toString(), "newBuilder", builderType.name().toString()));

                        var arg = returnHello.getMethodParam(0);

                        for (var f : jandex.getIndex().getClassByName(objJType.name().toString()).fields()) {
                            builder = returnHello.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(), "set" + capitalize(f.name()),
                                            builderType.name().toString(), f.type().toString()), builder, returnHello.readInstanceField(f, arg));
                        }

                        System.out.println("5");
                        var result = returnHello.invokeVirtualMethod(MethodDescriptor.ofMethod(builderType.name().toString(), "build", msgJType.name().toString()), builder);
                        System.out.println("6");

                        returnHello.returnValue(result);
                    }
                }

                return;
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
