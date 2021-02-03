/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isProtected;
import static java.lang.reflect.Modifier.isPublic;
import static java.util.Arrays.asList;
import static java.util.Collections.reverse;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.pojo.PojoSpecializationHelper.specializeTypeData;
import static org.bson.codecs.pojo.PropertyReflectionUtils.getPropertyMethods;
import static org.bson.codecs.pojo.PropertyReflectionUtils.isGetter;
import static org.bson.codecs.pojo.PropertyReflectionUtils.toPropertyName;

final class PojoBuilderHelper {
    static <T> void configureClassModelBuilder(final ClassModelBuilder<T> classModelBuilder, final Class<T> clazz) {
        classModelBuilder.type(notNull("clazz", clazz));

        if (isRecord(clazz)) {
            configureRecordClassModelBuilder(classModelBuilder, clazz);
        } else {
            configurePojoClassModelBuilder(classModelBuilder, clazz);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void configurePojoClassModelBuilder(final ClassModelBuilder<T> classModelBuilder, final Class<T> clazz) {
        classModelBuilder.isRecord(false);

        ArrayList<Annotation> annotations = new ArrayList<Annotation>();
        Set<String> propertyNames = new TreeSet<String>();
        Map<String, TypeParameterMap> propertyTypeParameterMap = new HashMap<String, TypeParameterMap>();
        Class<? super T> currentClass = clazz;
        String declaringClassName =  clazz.getSimpleName();
        TypeData<?> parentClassTypeData = null;

        Map<String, PropertyMetadata<?>> propertyNameMap = new HashMap<String, PropertyMetadata<?>>();
        while (!currentClass.isEnum() && currentClass.getSuperclass() != null) {
            annotations.addAll(asList(currentClass.getDeclaredAnnotations()));
            List<String> genericTypeNames = getGenericTypeNames(currentClass);

            PropertyReflectionUtils.PropertyMethods propertyMethods = getPropertyMethods(currentClass);

            // Note that we're processing setters before getters. It's typical for setters to have more general types
            // than getters (e.g.: getter returning ImmutableList, but setter accepting Collection), so by evaluating
            // setters first, we'll initialize the PropertyMetadata with the more general type
            for (Method method : propertyMethods.getSetterMethods()) {
                String propertyName = toPropertyName(method);
                propertyNames.add(propertyName);
                PropertyMetadata<?> propertyMetadata = getOrCreateMethodPropertyMetadata(propertyName, declaringClassName, propertyNameMap,
                        TypeData.newInstance(method), propertyTypeParameterMap, parentClassTypeData, genericTypeNames,
                        getGenericType(method));

                if (propertyMetadata.getSetter() == null) {
                    propertyMetadata.setSetter(method);
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        propertyMetadata.addWriteAnnotation(annotation);
                    }
                }
            }

            for (Method method : propertyMethods.getGetterMethods()) {
                String propertyName = toPropertyName(method);
                propertyNames.add(propertyName);
                // If the getter is overridden in a subclass, we only want to process that property, and ignore
                // potentially less specific methods from super classes
                PropertyMetadata<?> propertyMetadata = propertyNameMap.get(propertyName);
                if (propertyMetadata != null && propertyMetadata.getGetter() != null) {
                    continue;
                }
                propertyMetadata = getOrCreateMethodPropertyMetadata(propertyName, declaringClassName, propertyNameMap,
                                        TypeData.newInstance(method), propertyTypeParameterMap, parentClassTypeData, genericTypeNames,
                                        getGenericType(method));
                if (propertyMetadata.getGetter() == null) {
                    propertyMetadata.setGetter(method);
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        propertyMetadata.addReadAnnotation(annotation);
                    }
                }
            }

            for (Field field : currentClass.getDeclaredFields()) {
                propertyNames.add(field.getName());
                // Note if properties are present and types don't match, the underlying field is treated as an implementation detail.
                PropertyMetadata<?> propertyMetadata = getOrCreateFieldPropertyMetadata(field.getName(), declaringClassName,
                        propertyNameMap, TypeData.newInstance(field), propertyTypeParameterMap, parentClassTypeData, genericTypeNames,
                        field.getGenericType());
                if (propertyMetadata != null && propertyMetadata.getField() == null) {
                    propertyMetadata.field(field);
                    for (Annotation annotation : field.getDeclaredAnnotations()) {
                        propertyMetadata.addReadAnnotation(annotation);
                        propertyMetadata.addWriteAnnotation(annotation);
                    }
                }
            }

            parentClassTypeData = TypeData.newInstance(currentClass.getGenericSuperclass(), currentClass);
            currentClass = currentClass.getSuperclass();
        }

        if (currentClass.isInterface()) {
            annotations.addAll(asList(currentClass.getDeclaredAnnotations()));
        }

        for (String propertyName : propertyNames) {
            PropertyMetadata<?> propertyMetadata = propertyNameMap.get(propertyName);
            if (propertyMetadata.isSerializable() || propertyMetadata.isDeserializable()) {
                classModelBuilder.addProperty(createPropertyModelBuilder(propertyMetadata));
            }
        }

        reverse(annotations);
        classModelBuilder.annotations(annotations);
        classModelBuilder.propertyNameToTypeParameterMap(propertyTypeParameterMap);

        Constructor<T> noArgsConstructor = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (constructor.getParameterTypes().length == 0
                    && (isPublic(constructor.getModifiers()) || isProtected(constructor.getModifiers()))) {
                noArgsConstructor = (Constructor<T>) constructor;
                noArgsConstructor.setAccessible(true);
            }
        }
        classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(
                new ConstructorCreatorExecutable<>(clazz, noArgsConstructor)));
    }

    private static <T> void configureRecordClassModelBuilder(final ClassModelBuilder<T> classModelBuilder, final Class<T> clazz) {
        classModelBuilder.isRecord(true);

        Map<String, TypeParameterMap> propertyTypeParameterMap = new HashMap<>();
        String declaringClassName = clazz.getSimpleName();
        Map<String, PropertyMetadata<?>> propertyNameMap = new HashMap<>();

        List<String> genericTypeNames = getGenericTypeNames(clazz);

        List<Annotation> annotations = new ArrayList<>();
        annotations.addAll(asList(clazz.getDeclaredAnnotations()));

        RecordComponent[] recordComponents = clazz.getRecordComponents();
        Class<?>[] types = new Class<?>[recordComponents.length];
        for (int i = 0; i < recordComponents.length; i++) {
            RecordComponent recordComponent = recordComponents[i];
            String name = recordComponent.getName();
            types[i] = recordComponent.getType();

            Method getter;
            try {
                getter = clazz.getDeclaredMethod(name);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("Record must have getter method with same name as field", e);
            }
            PropertyMetadata<?> metadata = getOrCreateMethodPropertyMetadata(name, declaringClassName, propertyNameMap,
                    TypeData.newInstance(getter, true), propertyTypeParameterMap, null, genericTypeNames,
                    getGenericType(getter, true));

            metadata.setGetter(getter);

            for (Annotation a : recordComponent.getAnnotations()) {
                metadata.addWriteAnnotation(a);
                metadata.addReadAnnotation(a);
            }

            propertyNameMap.put(name, metadata);
            classModelBuilder.addProperty(createPropertyModelBuilder(metadata));
        }
        reverse(annotations);
        classModelBuilder.annotations(annotations);

        classModelBuilder.propertyNameToTypeParameterMap(propertyTypeParameterMap);

        Constructor<T> constructor;
        try {
            constructor = clazz.getConstructor(types);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Record must have a constructor that matches the record components", e);
        }
        classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(new RecordCreatorExecutable<T>(clazz, constructor)));
    }

    private static <T, S> PropertyMetadata<T> getOrCreateMethodPropertyMetadata(final String propertyName,
                                                                  final String declaringClassName,
                                                                  final Map<String, PropertyMetadata<?>> propertyNameMap,
                                                                  final TypeData<T> typeData,
                                                                  final Map<String, TypeParameterMap> propertyTypeParameterMap,
                                                                  final TypeData<S> parentClassTypeData,
                                                                  final List<String> genericTypeNames,
                                                                  final Type genericType) {
        PropertyMetadata<T> propertyMetadata = getOrCreatePropertyMetadata(propertyName, declaringClassName, propertyNameMap, typeData);
        if (!isAssignableClass(propertyMetadata.getTypeData().getType(), typeData.getType())) {
            propertyMetadata.setError(format("Property '%s' in %s, has differing data types: %s and %s.", propertyName,
                    declaringClassName, propertyMetadata.getTypeData(), typeData));
        }
        cachePropertyTypeData(propertyMetadata, propertyTypeParameterMap, parentClassTypeData, genericTypeNames, genericType);
        return propertyMetadata;
    }

    private static boolean isAssignableClass(final Class<?> propertyTypeClass, final Class<?> typeDataClass) {
        notNull("propertyTypeClass", propertyTypeClass);
        notNull("typeDataClass", typeDataClass);
        return propertyTypeClass.isAssignableFrom(typeDataClass) || typeDataClass.isAssignableFrom(propertyTypeClass);
    }

    private static <T, S> PropertyMetadata<T> getOrCreateFieldPropertyMetadata(final String propertyName,
                                                                               final String declaringClassName,
                                                                               final Map<String, PropertyMetadata<?>> propertyNameMap,
                                                                               final TypeData<T> typeData,
                                                                               final Map<String, TypeParameterMap> propertyTypeParameterMap,
                                                                               final TypeData<S> parentClassTypeData,
                                                                               final List<String> genericTypeNames,
                                                                               final Type genericType) {
        PropertyMetadata<T> propertyMetadata = getOrCreatePropertyMetadata(propertyName, declaringClassName, propertyNameMap, typeData);
        if (!propertyMetadata.getTypeData().getType().isAssignableFrom(typeData.getType())) {
            return null;
        }
        cachePropertyTypeData(propertyMetadata, propertyTypeParameterMap, parentClassTypeData, genericTypeNames, genericType);
        return propertyMetadata;
    }

    @SuppressWarnings("unchecked")
    private static <T> PropertyMetadata<T> getOrCreatePropertyMetadata(final String propertyName,
                                                                       final String declaringClassName,
                                                                       final Map<String, PropertyMetadata<?>> propertyNameMap,
                                                                       final TypeData<T> typeData) {
        PropertyMetadata<T> propertyMetadata = (PropertyMetadata<T>) propertyNameMap.get(propertyName);
        if (propertyMetadata == null) {
            propertyMetadata = new PropertyMetadata<T>(propertyName, declaringClassName, typeData);
            propertyNameMap.put(propertyName, propertyMetadata);
        }
        return propertyMetadata;
    }

    private static <T, S> void cachePropertyTypeData(final PropertyMetadata<T> propertyMetadata,
                                                     final Map<String, TypeParameterMap> propertyTypeParameterMap,
                                                     final TypeData<S> parentClassTypeData,
                                                     final List<String> genericTypeNames,
                                                     final Type genericType) {
        TypeParameterMap typeParameterMap = getTypeParameterMap(genericTypeNames, genericType);
        propertyTypeParameterMap.put(propertyMetadata.getName(), typeParameterMap);
        propertyMetadata.typeParameterInfo(typeParameterMap, parentClassTypeData);
    }

    private static Type getGenericType(final Method method) {
        return getGenericType(method, isGetter(method));
    }

    private static Type getGenericType(final Method method, final boolean isGetterMethod) {
        return isGetterMethod ? method.getGenericReturnType() : method.getGenericParameterTypes()[0];
    }

    @SuppressWarnings("unchecked")
    static <T> PropertyModelBuilder<T> createPropertyModelBuilder(final PropertyMetadata<T> propertyMetadata) {
        PropertyModelBuilder<T> propertyModelBuilder = PropertyModel.<T>builder()
                .propertyName(propertyMetadata.getName())
                .readName(propertyMetadata.getName())
                .writeName(propertyMetadata.getName())
                .typeData(propertyMetadata.getTypeData())
                .readAnnotations(propertyMetadata.getReadAnnotations())
                .writeAnnotations(propertyMetadata.getWriteAnnotations())
                .propertySerialization(new PropertyModelSerializationImpl<T>())
                .propertyAccessor(new PropertyAccessorImpl<T>(propertyMetadata))
                .setError(propertyMetadata.getError());

        if (propertyMetadata.getTypeParameters() != null) {
            propertyModelBuilder.typeData(specializeTypeData(propertyModelBuilder.getTypeData(), propertyMetadata.getTypeParameters(),
                    propertyMetadata.getTypeParameterMap()));
        }

        return propertyModelBuilder;
    }

    private static TypeParameterMap getTypeParameterMap(final List<String> genericTypeNames, final Type propertyType) {
        int classParamIndex = genericTypeNames.indexOf(propertyType.toString());
        TypeParameterMap.Builder builder = TypeParameterMap.builder();
        if (classParamIndex != -1) {
            builder.addIndex(classParamIndex);
        } else {
            if (propertyType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) propertyType;
                for (int i = 0; i < pt.getActualTypeArguments().length; i++) {
                    classParamIndex = genericTypeNames.indexOf(pt.getActualTypeArguments()[i].toString());
                    if (classParamIndex != -1) {
                        builder.addIndex(i, classParamIndex);
                    } else {
                        builder.addIndex(i, getTypeParameterMap(genericTypeNames, pt.getActualTypeArguments()[i]));
                    }
                }
            }
        }
        return builder.build();
    }

    private static <T> List<String> getGenericTypeNames(final Class<T> clazz) {
        List<String> genericTypeNames = new ArrayList<String>();
        for (TypeVariable<? extends Class<? super T>> classTypeVariable : clazz.getTypeParameters()) {
            genericTypeNames.add(classTypeVariable.getName());
        }
        return genericTypeNames;
    }

    static <V> V stateNotNull(final String property, final V value) {
        if (value == null) {
            throw new IllegalStateException(format("%s cannot be null", property));
        }
        return value;
    }

    static boolean isRecord(final Class<?> clazz) {
        try {
            // Getting the class of the class is intentional. isRecord is a method on the class, not the object
            return (Boolean) clazz.getClass().getMethod("isRecord").invoke(clazz);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
            return false;
        }
    }

    private PojoBuilderHelper() {
    }
}
