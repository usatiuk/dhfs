package com.usatiuk.objects.alloc.deployment;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.JData;
import com.usatiuk.objects.common.JObjectKey;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.gizmo.*;
import jakarta.inject.Singleton;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

class ObjectsAllocProcessor {
    @BuildStep
    void collectJDatas(BuildProducer<JDataIndexBuildItem> producer, ApplicationIndexBuildItem jandex) {
        var jdatas = jandex.getIndex().getAllKnownSubinterfaces(JData.class);

        // Collect the leaves
        for (var jdata : jdatas) {
            System.out.println("Found JData: " + jdata.name());
            if (jandex.getIndex().getAllKnownSubinterfaces(jdata.name()).isEmpty()) {
                System.out.println("Found JData leaf: " + jdata.name());
                producer.produce(new JDataIndexBuildItem(jdata));
            }
        }
    }


    JDataInfo collectData(JDataIndexBuildItem item) {
        var methodNameToInfo = item.jData.methods().stream()
                .collect(Collectors.toUnmodifiableMap(MethodInfo::name, x -> x));

        var reducableSet = new TreeSet<>(methodNameToInfo.keySet());

        var fields = new TreeMap<String, JDataFieldInfo>();

        // Find pairs of getters and setters
        // FIXME:
        while (!reducableSet.isEmpty()) {
            var name = reducableSet.first();
            reducableSet.remove(name);
            if (name.startsWith("get")) {
                var setterName = "set" + name.substring(3);
                if (reducableSet.contains(setterName)) {
                    reducableSet.remove(setterName);
                } else {
                    throw new RuntimeException("Missing setter for getter: " + name);
                }

                var getter = methodNameToInfo.get(name);
                var setter = methodNameToInfo.get(setterName);

                if (!getter.returnType().equals(setter.parameters().getFirst().type())) {
                    throw new RuntimeException("Getter and setter types do not match: " + name);
                }

                var variableName = name.substring(3, 4).toLowerCase() + name.substring(4);

                fields.put(variableName, new JDataFieldInfo(variableName, getter.returnType()));
            } else {
                throw new RuntimeException("Unknown method name: " + name);
            }
        }

        return new JDataInfo(item.jData, Collections.unmodifiableMap(fields));
    }

    interface TypeFunction {
        void apply(Type type);
    }

    void matchClass(BytecodeCreator bytecodeCreator, ResultHandle value, List<Type> types, TypeFunction fn) {
//        bytecodeCreator.insta
    }

    interface ClassTagFunction {
        void apply(ClassInfo type, BytecodeCreator branch);
    }

    // Returns false branch
    <T> BytecodeCreator matchClassTag(BytecodeCreator bytecodeCreator, ResultHandle toMatch, List<ClassInfo> types, ClassTagFunction fn) {
        if (types.isEmpty()) {
            return bytecodeCreator;
        }

        var eq = bytecodeCreator.invokeVirtualMethod(
                MethodDescriptor.ofMethod(Object.class, "equals", boolean.class, Object.class),
                toMatch,
                bytecodeCreator.loadClass(types.getFirst())
        );

        var cmp = bytecodeCreator.ifTrue(eq);
        fn.apply(types.getFirst(), cmp.trueBranch());
        return matchClassTag(cmp.falseBranch(), toMatch, types.subList(1, types.size()), fn);
    }

    @BuildStep
    void makeJDataThingy(List<JDataIndexBuildItem> jDataItems,
                         BuildProducer<GeneratedBeanBuildItem> generatedBeans,
                         BuildProducer<GeneratedClassBuildItem> generatedClasses) {

        var data = jDataItems.stream().map(this::collectData).collect(Collectors.toUnmodifiableMap(JDataInfo::klass, x -> x));
        var classes = data.keySet().stream().map(ClassInfo::asClass).toList();

        var gizmoAdapter = new GeneratedBeanGizmoAdaptor(generatedBeans);

        try (ClassCreator classCreator = ClassCreator.builder()
                .className("com.usatiuk.objects.alloc.generated.ObjectAllocatorImpl")
                .interfaces(ObjectAllocator.class)
                .classOutput(gizmoAdapter)
                .build()) {

            classCreator.addAnnotation(Singleton.class);

            try (MethodCreator methodCreator = classCreator.getMethodCreator("create", JData.class, Class.class, JObjectKey.class)) {
                matchClassTag(methodCreator, methodCreator.getMethodParam(0), classes, (type, branch) -> {
                    branch.returnValue(branch.newInstance(MethodDescriptor.ofConstructor(type.toString(), JObjectKey.class), branch.getMethodParam(1)));
                });
                methodCreator.throwException(IllegalArgumentException.class, "Unknown type");
            }

            try (MethodCreator methodCreator = classCreator.getMethodCreator("copy", ObjectAllocator.ChangeTrackingJData.class, JData.class)) {
                methodCreator.returnValue(methodCreator.loadNull());
            }

            try (MethodCreator methodCreator = classCreator.getMethodCreator("unmodifiable", JData.class, JData.class)) {
                methodCreator.returnValue(methodCreator.loadNull());
            }
        }

    }
}
