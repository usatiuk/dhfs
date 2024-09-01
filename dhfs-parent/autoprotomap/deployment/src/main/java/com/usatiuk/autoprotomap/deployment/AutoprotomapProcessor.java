package com.usatiuk.autoprotomap.deployment;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.SignatureBuilder;
import jakarta.inject.Singleton;
import org.jboss.jandex.ClassType;
import org.jboss.jandex.Type;

class AutoprotomapProcessor {
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

    @BuildStep
    void generateProtoSerializer(ApplicationIndexBuildItem jandex,
                                 ProtoIndexBuildItem protoIndex,
                                 BuildProducer<GeneratedBeanBuildItem> generatedClasses) {
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

                    var generator = new ProtoSerializerGenerator(
                            jandex.getIndex(),
                            protoIndex,
                            classCreator,
                            msgJType,
                            objJType
                    );

                    generator.generate();
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
