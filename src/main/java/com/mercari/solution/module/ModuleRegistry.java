package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModuleRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ModuleRegistry.class);

    private static final ModuleRegistry instance = new ModuleRegistry();

    private final Map<String, SourceModule> sources = new HashMap<>();
    private final Map<String, TransformModule> transforms = new HashMap<>();
    private final Map<String, SinkModule> sinks = new HashMap<>();

    public static ModuleRegistry getInstance() {
        return instance;
    }

    private ModuleRegistry() {
        registerBuiltinModules();
    }

    private void registerBuiltinModules() {
        List<Class<? extends SourceModule>> sourceModules = findModulesInPackage("com.mercari.solution.module.source", SourceModule.class);
        sourceModules.forEach(this::registerSource);

        List<Class<? extends TransformModule>> transformModules = findModulesInPackage("com.mercari.solution.module.transform", TransformModule.class);
        transformModules.forEach(this::registerTransform);

        List<Class<? extends SinkModule>> sinkModules = findModulesInPackage("com.mercari.solution.module.sink", SinkModule.class);
        sinkModules.forEach(this::registerSink);
    }

    public void registerSource(Class<? extends SourceModule> moduleClass) {
        registerModule(sources, moduleClass);
    }

    public void registerTransform(Class<? extends TransformModule> moduleClass) {
        registerModule(transforms, moduleClass);
    }

    public void registerSink(Class<? extends SinkModule> moduleClass) {
        registerModule(sinks, moduleClass);
    }

    public SourceModule getSource(String name) {
        return sources.get(name);
    }

    public TransformModule getTransform(String name) {
        return transforms.get(name);
    }

    public SinkModule getSink(String name) {
        return sinks.get(name);
    }

    private <T extends Module> List<Class<? extends T>> findModulesInPackage(String packageName, Class<T> moduleClass) {
        ClassPath classPath;
        try {
            ClassLoader loader = getClass().getClassLoader();
            classPath = ClassPath.from(loader);
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        List<Class<? extends T>> modules = classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(moduleClass::isAssignableFrom)
                .map(clazz -> (Class<? extends T>)clazz.asSubclass(moduleClass))
                .collect(Collectors.toList());
        return modules;
    }

    private <T extends Module> void registerModule(Map<String, T> container, Class<? extends T> moduleClass) {
        T module;
        try {
            module = moduleClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate module: " + moduleClass, e);
        }
        if (container.containsKey(module.getName())) {
            throw new IllegalStateException("Module is already registered: " + module.getName());
        }
        container.put(module.getName(), module);
        LOG.debug("registered module: " + module.getName());
    }

}
