package com.indeed.iql2.language.transform;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TransformTestUtil {
    private TransformTestUtil() {
    }

    static List<Class> findImplementationsOf(final Class<?> target) throws IOException, ClassNotFoundException {
        final ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
        final MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourcePatternResolver);

        final List<Class> candidates = new ArrayList<>();
        final String packageSearchPath = "classpath*:com/indeed/iql2/language/**/*.class";
        final Resource[] resources = resourcePatternResolver.getResources(packageSearchPath);
        for (final Resource resource : resources) {
            if (resource.isReadable()) {
                final MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
                if (isCandidate(metadataReader, target)) {
                    candidates.add(Class.forName(metadataReader.getClassMetadata().getClassName()));
                }
            }
        }
        return candidates;
    }

    private static boolean isCandidate(final MetadataReader metadataReader, final Class<?> target) {
        try {
            final Class c = Class.forName(metadataReader.getClassMetadata().getClassName());
            if (c.isInterface()) {
                // skip interfaces
                return false;
            }
            if (Modifier.isAbstract(c.getModifiers())) {
                // skip abstract classes
                return false;
            }
            if (extendsOrImplements(c, target)) {
                return true;
            }
        } catch(final Throwable ignored){
        }
        return false;
    }

    private static boolean extendsOrImplements(final Class<?> candidate, final Class<?> target) {
        if (target.isInterface()) {
            return isImplementationOf(candidate, target);
        } else {
            return isExtensionOf(candidate, target);
        }
    }

    private static boolean isImplementationOf(final Class<?> candidate, final Class<?> target) {
        return Arrays.asList(candidate.getInterfaces()).contains(target);
    }

    private static boolean isExtensionOf(Class<?> candidate, final Class<?> target) {
        while (true) {
            if (candidate.equals(target)) {
                return true;
            }
            if (candidate.equals(Object.class)) {
                return false;
            }
            candidate = candidate.getSuperclass();
        }
    }
}
