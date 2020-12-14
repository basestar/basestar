package io.basestar.schema.use;

import io.basestar.exception.InvalidDateTimeException;
import io.basestar.expression.type.Values;
import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.schema.*;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.secret.Secret;
import io.basestar.secret.SecretContext;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public interface ValueContext {

    static ValueContext standard() {

        return Standard.INSTANCE;
    }

    static ValueContext suppressing() {

        return Suppressing.INSTANCE;
    }

    static ValueContext standardOrSuppressing(boolean suppress) {

        return suppress ? ValueContext.suppressing() : ValueContext.standard();
    }

    static ValueContext encrypting(final SecretContext secretContext) {

        return new Encrypting(secretContext);
    }

    static ValueContext decrypting(final SecretContext secretContext) {

        return new Decrypting(secretContext);
    }

    Object createAny(UseAny type, Object value, Set<Name> expand);

    Boolean createBoolean(UseBoolean type, Object value, Set<Name> expand);

    String createString(UseString type, Object value, Set<Name> expand);

    Double createNumber(UseNumber type, Object value, Set<Name> expand);

    Long createInteger(UseInteger type, Object value, Set<Name> expand);

    <T> List<T> createArray(UseArray<T> type, Object value, Set<Name> expand);

    <T> Set<T> createSet(UseSet<T> type, Object value, Set<Name> expand);

    <T> Map<String, T> createMap(UseMap<T> type, Object value, Set<Name> expand);

    byte[] createBinary(UseBinary useBinary, Object value, Set<Name> expand);

    LocalDate createDate(UseDate type, Object value, Set<Name> expand);

    Instant createDateTime(UseDateTime type, Object value, Set<Name> expand);

    String createEnum(UseEnum type, Object value, Set<Name> expand);

    Instance createStruct(UseStruct type, Object value, Set<Name> expand);

    Instance createRef(UseRef type, Object value, Set<Name> expand);

    Instance createView(UseView type, Object value, Set<Name> expand);

    Secret createSecret(UseSecret type, Object value, Set<Name> expand);

    <T> Page<T> createPage(UsePage<T> type, Object value, Set<Name> expand);

    class Standard implements ValueContext {

        public static Standard INSTANCE = new Standard();

        @Override
        public LocalDate createDate(final UseDate type, final Object value, final Set<Name> expand) {

            if(value instanceof LocalDate) {
                return (LocalDate)value;
            }
            try {
                return ISO8601.toDate(value);
            } catch (final InvalidDateTimeException e) {
                throw new TypeConversionException(LocalDate.class, value);
            }
        }

        @Override
        public Instant createDateTime(final UseDateTime type, final Object value, final Set<Name> expand) {

            if(value instanceof Instant) {
                return (Instant)value;
            }
            try {
                return ISO8601.toDateTime(value);
            } catch (final InvalidDateTimeException e) {
                throw new TypeConversionException(LocalDate.class, value);
            }
        }

        @Override
        public String createEnum(final UseEnum type, final Object value, final Set<Name> expand) {

            final EnumSchema schema = type.getSchema();
            return schema.create(this, value, expand);
        }

        @Override
        public Instance createStruct(final UseStruct type, final Object value, final Set<Name> expand) {

            final StructSchema schema = type.getSchema();
            return schema.create(this, value, expand);
        }

        @Override
        public Instance createRef(final UseRef type, final Object value, final Set<Name> expand) {

            if(value instanceof Map) {
                final ReferableSchema schema = type.getSchema();
                final boolean versioned = type.isVersioned();
                @SuppressWarnings("unchecked")
                final Map<String, Object> map = (Map<String, Object>)value;
                final String id = Instance.getId(map);
                if (id == null) {
                    return null;
                } else {
                    if(expand != null && ReferableSchema.isResolved(map)) {
                        return schema.create(this, map, expand);
                    } else {
                        if(versioned) {
                            final Long version = Instance.getVersion(map);
                            if(version == null) {
                                throw new UnexpectedTypeException(type, value);
                            }
                            return ReferableSchema.versionedRef(id, version);
                        } else {
                            return ReferableSchema.ref(id);
                        }
                    }
                }
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        }

        @Override
        public Instance createView(final UseView type, final Object value, final Set<Name> expand) {

            final ViewSchema schema = type.getSchema();
            return schema.create(this, value, expand);
        }

        @Override
        public Secret createSecret(final UseSecret secret, final Object value, final Set<Name> expand) {

            if(value instanceof Secret) {
                return (Secret) value;
            } else if(value instanceof byte[]) {
                return Secret.encrypted((byte[])value);
            } else {
                throw new TypeConversionException(Secret.class, "<redacted>");
            }
        }

        @Override
        public Object createAny(final UseAny type, Object value, final Set<Name> expand) {

            return value;
        }

        @Override
        public Boolean createBoolean(final UseBoolean type, final Object value, final Set<Name> expand) {

            return Values.toBoolean(value);
        }

        @Override
        public String createString(final UseString type, final Object value, final Set<Name> expand) {

            return Values.toString(value);
        }

        @Override
        public Double createNumber(final UseNumber type, final Object value, final Set<Name> expand) {

            return Values.toFloat(value);
        }

        @Override
        public Long createInteger(final UseInteger type, final Object value, final Set<Name> expand) {

            return Values.toInteger(value);
        }

        @Override
        public <T> List<T> createArray(final UseArray<T> type, final Object value, final Set<Name> expand) {

            if(value instanceof Collection) {
                final Use<T> valueType = type.getType();
                return ((Collection<?>) value).stream()
                        .map(v -> valueType.create(this, v, expand))
                        .collect(Collectors.toList());
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        }

        @Override
        public <T> Set<T> createSet(final UseSet<T> type, final Object value, final Set<Name> expand) {

            if(value instanceof Collection) {
                final Use<T> valueType = type.getType();
                return ((Collection<?>) value).stream()
                        .map(v -> valueType.create(this, v, expand))
                        .collect(Collectors.toSet());
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        }

        @Override
        public <T> Map<String, T> createMap(final UseMap<T> type, final Object value, final Set<Name> expand) {

            if(value instanceof Map) {
                final Map<String, Set<Name>> branches = Name.branch(expand);
                final Use<T> valueType = type.getType();
                final Map<String, T> result = new HashMap<>();
                ((Map<?, ?>) value).forEach((k, v) -> {
                    final String key = k.toString();
                    result.put(key, valueType.create(this, v, UseMap.branch(branches, key)));
                });
                return result;
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        }

        @Override
        public byte[] createBinary(final UseBinary useBinary, final Object value, final Set<Name> expand) {

            return Values.toBinary(value);
        }

        @Override
        public <T> Page<T> createPage(final UsePage<T> type, final Object value, final Set<Name> expand) {

            if(value instanceof Collection) {
                final Use<T> valueType = type.getType();
                final List<T> values = ((Collection<?>) value).stream()
                        .map(v -> valueType.create(this, v, expand))
                        .collect(Collectors.toList());
                if(value instanceof Page) {
                    final Page<?> page = (Page<?>)value;
                    return new Page<>(values, page.getPaging(), page.getStats());
                } else {
                    return new Page<>(values, null);
                }
            } else {
                throw new UnexpectedTypeException(type, value);
            }
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    class Suppressing extends Standard {

        public static Suppressing INSTANCE = new Suppressing();

        @Override
        public Object createAny(final UseAny type, final Object value, final Set<Name> expand) {

            try {
                return super.createAny(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Boolean createBoolean(final UseBoolean type, final Object value, final Set<Name> expand) {

            try {
                return super.createBoolean(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public String createString(final UseString type, final Object value, final Set<Name> expand) {

            try {
                return super.createString(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Double createNumber(final UseNumber type, final Object value, final Set<Name> expand) {

            try {
                return super.createNumber(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Long createInteger(final UseInteger type, final Object value, final Set<Name> expand) {

            try {
                return super.createInteger(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public <T> List<T> createArray(final UseArray<T> type, final Object value, final Set<Name> expand) {

            try {
                return super.createArray(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public <T> Page<T> createPage(final UsePage<T> type, final Object value, final Set<Name> expand) {

            try {
                return super.createPage(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public <T> Set<T> createSet(final UseSet<T> type, final Object value, final Set<Name> expand) {

            try {
                return super.createSet(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public <T> Map<String, T> createMap(final UseMap<T> type, final Object value, final Set<Name> expand) {

            try {
                return super.createMap(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public byte[] createBinary(final UseBinary type, final Object value, final Set<Name> expand) {

            try {
                return super.createBinary(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public LocalDate createDate(final UseDate type, final Object value, final Set<Name> expand) {

            try {
                return super.createDate(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Instant createDateTime(final UseDateTime type, final Object value, final Set<Name> expand) {

            try {
                return super.createDateTime(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public String createEnum(final UseEnum type, final Object value, final Set<Name> expand) {

            try {
                return super.createEnum(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Instance createStruct(final UseStruct type, final Object value, final Set<Name> expand) {

            try {
                return super.createStruct(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Instance createRef(final UseRef type, final Object value, final Set<Name> expand) {

            try {
                return super.createRef(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Instance createView(final UseView type, final Object value, final Set<Name> expand) {

            try {
                return super.createView(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }

        @Override
        public Secret createSecret(final UseSecret type, final Object value, final Set<Name> expand) {

            try {
                return super.createSecret(type, value, expand);
            } catch (final UnexpectedTypeException | TypeConversionException | ConstraintViolationException e) {
                log.warn("Suppressing type conversion error", e);
                return null;
            }
        }
    }

    class Encrypting extends Standard {

        private final SecretContext secretContext;

        public Encrypting(final SecretContext secretContext) {

            this.secretContext = secretContext;
        }

        @Override
        public Secret createSecret(final UseSecret secret, final Object value, final Set<Name> expand) {

            if(value instanceof Secret.Plaintext) {
                return secretContext.encrypt((Secret.Plaintext)value).join();
            } else if(value instanceof String) {
                return secretContext.encrypt(Secret.plaintext((String) value)).join();
            } else {
                throw new TypeConversionException(Secret.class, "<redacted>");
            }
        }
    }

    class Decrypting extends Standard {

        private final SecretContext secretContext;

        public Decrypting(final SecretContext secretContext) {

            this.secretContext = secretContext;
        }

        @Override
        public Secret createSecret(final UseSecret secret, final Object value, final Set<Name> expand) {

            if(value instanceof Secret.Encrypted) {
                return secretContext.decrypt((Secret.Encrypted) value).join();
            } else if(value instanceof byte[]) {
                return secretContext.decrypt(Secret.encrypted((byte[])value)).join();
            } else if(value instanceof String) {
                return secretContext.decrypt(Secret.encrypted((String)value)).join();
            } else {
                throw new TypeConversionException(Secret.class, "<redacted>");
            }
        }
    }
}
