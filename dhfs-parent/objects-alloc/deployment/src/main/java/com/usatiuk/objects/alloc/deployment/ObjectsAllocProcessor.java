package com.usatiuk.objects.alloc.deployment;

import com.usatiuk.objects.alloc.runtime.ChangeTrackingJData;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JDataAllocVersionProvider;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.gizmo.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.*;

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

    private static final String KEY_NAME = "key";
    private static final String VERSION_NAME = "version";
    private static final List<String> SPECIAL_FIELDS = List.of(KEY_NAME, VERSION_NAME);

    String propNameToFieldName(String name) {
        return name;
    }

    String propNameToGetterName(String name) {
        return "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    String propNameToSetterName(String name) {
        return "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    DotName getDataClassName(ClassInfo jData) {
        return DotName.createComponentized(jData.name().packagePrefixName(), jData.name().local() + "Data");
    }

    DotName getCTClassName(ClassInfo jData) {
        return DotName.createComponentized(jData.name().packagePrefixName(), jData.name().local() + "CTData");
    }

    DotName getImmutableClassName(ClassInfo jData) {
        return DotName.createComponentized(jData.name().packagePrefixName(), jData.name().local() + "ImmutableData");
    }

    @BuildStep
    void generateDataClass(List<JDataInfoBuildItem> jDataItems, BuildProducer<GeneratedClassBuildItem> generatedClasses) {
        var gizmoAdapter = new GeneratedClassGizmoAdaptor(generatedClasses, true);
        for (var item : jDataItems) {
            try (ClassCreator classCreator = ClassCreator.builder()
                    .className(getDataClassName(item.klass).toString())
                    .interfaces(JData.class)
                    .interfaces(item.klass.name().toString())
                    .interfaces(Serializable.class)
                    .classOutput(gizmoAdapter)
                    .build()) {


                var fieldsMap = createFields(item, classCreator);

                for (var field : fieldsMap.values()) {
                    if (!SPECIAL_FIELDS.contains(field.getName())) {
                        try (var setter = classCreator.getMethodCreator(propNameToSetterName(field.getName()), void.class, field.getType())) {
                            setter.writeInstanceField(field, setter.getThis(), setter.getMethodParam(0));
                            setter.returnVoid();
                        }
                    }
                }

                try (var constructor = classCreator.getConstructorCreator(JObjectKey.class, long.class)) {
                    constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                    constructor.writeInstanceField(fieldsMap.get(KEY_NAME), constructor.getThis(), constructor.getMethodParam(0));
                    constructor.writeInstanceField(fieldsMap.get(VERSION_NAME), constructor.getThis(), constructor.getMethodParam(1));
                    constructor.returnVoid();
                }

            }
        }
    }

    private static final String MODIFIED_FIELD_NAME = "_modified";
    private static final String ON_CHANGE_METHOD_NAME = "onChange";

    @BuildStep
    void generateCTClass(List<JDataInfoBuildItem> jDataItems, BuildProducer<GeneratedClassBuildItem> generatedClasses) {
        var gizmoAdapter = new GeneratedClassGizmoAdaptor(generatedClasses, true);
        for (var item : jDataItems) {
            try (ClassCreator classCreator = ClassCreator.builder()
                    .className(getCTClassName(item.klass).toString())
                    .interfaces(JData.class, ChangeTrackingJData.class)
                    .interfaces(item.klass.name().toString())
                    .interfaces(Serializable.class)
                    .classOutput(gizmoAdapter)
                    .build()) {
                var modified = classCreator.getFieldCreator(MODIFIED_FIELD_NAME, boolean.class);
                modified.setModifiers(PRIVATE | TRANSIENT);

                try (var modifiedGetter = classCreator.getMethodCreator("isModified", boolean.class)) {
                    modifiedGetter.returnValue(modifiedGetter.readInstanceField(modified.getFieldDescriptor(), modifiedGetter.getThis()));
                }

                try (var onChanged = classCreator.getMethodCreator(ON_CHANGE_METHOD_NAME, void.class)) {
                    onChanged.writeInstanceField(modified.getFieldDescriptor(), onChanged.getThis(), onChanged.load(true));
                    onChanged.returnVoid();
                }

                try (var wrapped = classCreator.getMethodCreator("wrapped", item.klass.name().toString())) {
                    wrapped.returnValue(wrapped.getThis());
                }

                try (var wrapped = classCreator.getMethodCreator("wrapped", JData.class)) {
                    wrapped.returnValue(wrapped.getThis());
                }

                var fieldsMap = createFields(item, classCreator);

                for (var field : fieldsMap.values()) {
                    if (!SPECIAL_FIELDS.contains(field.getName())) {
                        try (var setter = classCreator.getMethodCreator(propNameToSetterName(field.getName()), void.class, field.getType())) {
                            setter.writeInstanceField(field, setter.getThis(), setter.getMethodParam(0));
                            setter.invokeVirtualMethod(MethodDescriptor.ofMethod(classCreator.getClassName(), ON_CHANGE_METHOD_NAME, void.class), setter.getThis());
                            setter.returnVoid();
                        }
                    }
                }

                try (var constructor = classCreator.getConstructorCreator(item.klass.name().toString(), long.class.getName())) {
                    constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                    constructor.writeInstanceField(modified.getFieldDescriptor(), constructor.getThis(), constructor.load(false));
                    for (var field : fieldsMap.values()) {
                        if (!Objects.equals(field.getName(), VERSION_NAME))
                            constructor.writeInstanceField(field, constructor.getThis(), constructor.invokeInterfaceMethod(
                                    MethodDescriptor.ofMethod(item.klass.name().toString(), propNameToGetterName(field.getName()), field.getType()),
                                    constructor.getMethodParam(0)
                            ));
                    }
                    constructor.writeInstanceField(fieldsMap.get(VERSION_NAME), constructor.getThis(), constructor.getMethodParam(1));
                    constructor.returnVoid();
                }
            }
        }

    }

    @BuildStep
    void generateImmutableClass(List<JDataInfoBuildItem> jDataItems, BuildProducer<GeneratedClassBuildItem> generatedClasses) {
        var gizmoAdapter = new GeneratedClassGizmoAdaptor(generatedClasses, true);
        for (var item : jDataItems) {
            try (ClassCreator classCreator = ClassCreator.builder()
                    .className(getImmutableClassName(item.klass).toString())
                    .interfaces(JData.class, ChangeTrackingJData.class)
                    .interfaces(item.klass.name().toString())
                    .interfaces(Serializable.class)
                    .classOutput(gizmoAdapter)
                    .build()) {

                var fieldsMap = createFields(item, classCreator);

                for (var field : fieldsMap.values()) {
                    try (var setter = classCreator.getMethodCreator(propNameToSetterName(field.getName()), void.class, field.getType())) {
                        setter.throwException(UnsupportedOperationException.class, "Immutable object");
                    }
                }

                try (var constructor = classCreator.getConstructorCreator(item.klass.name().toString())) {
                    constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                    for (var field : fieldsMap.values()) {
                        constructor.writeInstanceField(field, constructor.getThis(), constructor.invokeInterfaceMethod(
                                MethodDescriptor.ofMethod(item.klass.name().toString(), propNameToGetterName(field.getName()), field.getType()),
                                constructor.getMethodParam(0)
                        ));
                    }
                    constructor.returnVoid();
                }
            }
        }

    }

    private Map<String, FieldDescriptor> createFields(JDataInfoBuildItem item, ClassCreator classCreator) {
        return item.fields.values().stream().map(jDataFieldInfo -> {
            var fc = classCreator.getFieldCreator(propNameToFieldName(jDataFieldInfo.name()), jDataFieldInfo.type().toString());

            if (SPECIAL_FIELDS.contains(jDataFieldInfo.name())) {
                fc.setModifiers(PRIVATE | FINAL);
            } else {
                fc.setModifiers(PRIVATE);
            }

            try (var getter = classCreator.getMethodCreator(propNameToGetterName(jDataFieldInfo.name()), jDataFieldInfo.type().toString())) {
                getter.returnValue(getter.readInstanceField(fc.getFieldDescriptor(), getter.getThis()));
            }
            return Pair.of(jDataFieldInfo, fc.getFieldDescriptor());
        }).collect(Collectors.toUnmodifiableMap(i -> i.getLeft().name(), Pair::getRight));
    }

    List<ClassInfo> collectInterfaces(ClassInfo type, ApplicationIndexBuildItem jandex) {
        return Stream.concat(Stream.of(type), type.interfaceNames().stream()
                        .flatMap(x -> {
                            var ret = jandex.getIndex().getClassByName(x);
                            if (ret == null) {
                                System.out.println("Interface not found! " + x);
                                return Stream.empty();
                            }
                            return Stream.of(ret);
                        })
                        .flatMap(i -> collectInterfaces(i, jandex).stream()))
                .collect(Collectors.toList());
    }

    Map<String, MethodInfo> collectMethods(List<ClassInfo> types) {
        return types.stream()
                .flatMap(x -> x.methods().stream())
                .collect(Collectors.toMap(MethodInfo::name, x -> x));
    }

    @BuildStep
    void collectData(BuildProducer<JDataInfoBuildItem> producer, List<JDataIndexBuildItem> items, ApplicationIndexBuildItem jandex) {
        for (var item : items) {
            var methodNameToInfo = collectMethods(collectInterfaces(item.jData, jandex));

            var reducableSet = new TreeSet<>(methodNameToInfo.keySet());

            var fields = new TreeMap<String, JDataFieldInfo>();
            if (reducableSet.contains(propNameToGetterName(KEY_NAME))) {
                reducableSet.remove(propNameToGetterName(KEY_NAME));
                var methodInfo = methodNameToInfo.get(propNameToGetterName(KEY_NAME));
                if (!methodInfo.returnType().name().equals(DotName.createSimple(JObjectKey.class.getName()))) {
                    throw new RuntimeException("Key getter must return JObjectKey");
                }
                fields.put(KEY_NAME, new JDataFieldInfo(KEY_NAME, methodNameToInfo.get(propNameToGetterName(KEY_NAME)).returnType().name()));
            } else {
//                throw new RuntimeException("Missing key getter");
                System.out.println("Missing key getter for " + item.jData);
                // FIXME!: No matter what, I couldn't get JData to get indexed by jandex
                fields.put(KEY_NAME, new JDataFieldInfo(KEY_NAME, DotName.createSimple(JObjectKey.class)));
                fields.put(VERSION_NAME, new JDataFieldInfo(VERSION_NAME, DotName.createSimple(long.class)));
            }

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

                    fields.put(variableName, new JDataFieldInfo(variableName, getter.returnType().name()));
                } else {
                    throw new RuntimeException("Unknown method name: " + name);
                }
            }
            producer.produce(new JDataInfoBuildItem(item.jData, Collections.unmodifiableMap(fields)));
        }
    }

    // Returns false branch
    void matchClass(BytecodeCreator bytecodeCreator, ResultHandle toMatch, List<ClassInfo> types, ClassTagFunction fn) {
        for (var type : types) {
            var eq = bytecodeCreator.instanceOf(toMatch, type.name().toString());
            var cmp = bytecodeCreator.ifTrue(eq);
            fn.apply(type, cmp.trueBranch(), toMatch);
        }
    }

    interface ClassTagFunction {
        void apply(ClassInfo type, BytecodeCreator branch, ResultHandle value);
    }

    // Returns false branch
    void matchClassTag(BytecodeCreator bytecodeCreator, ResultHandle toMatch, List<ClassInfo> types, ClassTagFunction fn) {
        for (var type : types) {
            var eq = bytecodeCreator.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Object.class, "equals", boolean.class, Object.class),
                    toMatch,
                    bytecodeCreator.loadClass(type.name().toString())
            );

            var cmp = bytecodeCreator.ifTrue(eq);
            fn.apply(type, cmp.trueBranch(), toMatch);
        }
    }

    @BuildStep
    void makeJDataThingy(List<JDataInfoBuildItem> jDataItems,
                         BuildProducer<GeneratedBeanBuildItem> generatedBeans) {
        var data = jDataItems.stream().collect(Collectors.toUnmodifiableMap(i -> i.klass, x -> x));
        var classes = data.keySet().stream().map(ClassInfo::asClass).toList();

        var gizmoAdapter = new GeneratedBeanGizmoAdaptor(generatedBeans);

        try (ClassCreator classCreator = ClassCreator.builder()
                .className("com.usatiuk.objects.alloc.generated.ObjectAllocatorImpl")
                .interfaces(ObjectAllocator.class)
                .classOutput(gizmoAdapter)
                .build()) {

            classCreator.addAnnotation(Singleton.class);

            var versionProvider = classCreator.getFieldCreator("versionProvider", JDataAllocVersionProvider.class);
            versionProvider.addAnnotation(Inject.class);
            versionProvider.setModifiers(PUBLIC);

            Function<BytecodeCreator, ResultHandle> loadVersion = (block) -> block.invokeInterfaceMethod(
                    MethodDescriptor.ofMethod(JDataAllocVersionProvider.class, "getVersion", long.class),
                    block.readInstanceField(versionProvider.getFieldDescriptor(), block.getThis())
            );

            try (MethodCreator methodCreator = classCreator.getMethodCreator("create", JData.class, Class.class, JObjectKey.class)) {
                matchClassTag(methodCreator, methodCreator.getMethodParam(0), classes, (type, branch, value) -> {
                    branch.returnValue(branch.newInstance(MethodDescriptor.ofConstructor(getDataClassName(type).toString(), JObjectKey.class, long.class), branch.getMethodParam(1), loadVersion.apply(branch)));
                });
                methodCreator.throwException(IllegalArgumentException.class, "Unknown type");
            }

            try (MethodCreator methodCreator = classCreator.getMethodCreator("copy", ChangeTrackingJData.class, JData.class)) {
                matchClass(methodCreator, methodCreator.getMethodParam(0), classes, (type, branch, value) -> {
                    branch.returnValue(branch.newInstance(MethodDescriptor.ofConstructor(getCTClassName(type).toString(), type.name().toString(), long.class.getName()), value, loadVersion.apply(branch)));
                });
                methodCreator.throwException(IllegalArgumentException.class, "Unknown type");
            }

            try (MethodCreator methodCreator = classCreator.getMethodCreator("unmodifiable", JData.class, JData.class)) {
                matchClass(methodCreator, methodCreator.getMethodParam(0), classes, (type, branch, value) -> {
                    branch.returnValue(branch.newInstance(MethodDescriptor.ofConstructor(getImmutableClassName(type).toString(), type.name().toString()), value));
                });
                methodCreator.throwException(IllegalArgumentException.class, "Unknown type");
            }
        }

    }
}
