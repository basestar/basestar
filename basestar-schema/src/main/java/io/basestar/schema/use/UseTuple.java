//package io.basestar.schema.use;
//
//import com.google.common.collect.HashMultimap;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.Multimap;
//import io.basestar.schema.Expander;
//import io.basestar.schema.Instance;
//import io.basestar.schema.Namespace;
//import io.basestar.schema.exception.InvalidTypeException;
//import Tuple;
//import io.basestar.util.Path;
//import lombok.Data;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.*;
//import java.util.stream.Collectors;
//
//@Data
//public class UseTuple implements Use<Tuple> {
//
//    public static final String NAME = "tuple";
//
//    private final List<Use<?>> types;
//
//    public static UseTuple from(final Object config) {
//
//        final List<Use<?>> types;
//        if(config instanceof Collection<?>) {
//            types = (((Collection<?>) config).stream())
//                    .map(Use::from).collect(Collectors.toList());
//        } else {
//            throw new InvalidTypeException();
//        }
//        return new UseTuple(types);
//    }
//
//    @Override
//    public <R> R visit(final Visitor<R> visitor) {
//
//        return visitor.visitTuple(this);
//    }
//
//    @Override
//    public Object toJson() {
//
//        return ImmutableMap.of(
//                NAME, types
//        );
//    }
//
//    @Override
//    public UseTuple resolve(final Namespace namespace) {
//
//        return new UseTuple(types.stream()
//                .map(v -> v.resolve(namespace))
//                .collect(Collectors.toList()));
//    }
//
//    @Override
//    public Tuple create(final Object value) {
//
//        if(value == null) {
//            return null;
//        } else if(value instanceof Collection) {
//            final List<Object> values = new ArrayList<>();
//            final Iterator iter = ((Collection) value).iterator();
//            for(final Use<?> type : types) {
//                if(iter.hasNext()) {
//                    values.add(type.create(iter.next()));
//                } else {
//                    values.add(null);
//                }
//            }
//            return new Tuple(values);
//        } else {
//            throw new InvalidTypeException();
//        }
//    }
//
//    @Override
//    public Code code() {
//
//        return Code.TUPLE;
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public void serializeValue(final Tuple value, final DataOutput out) throws IOException {
//
//        final Iterator iter = value.iterator();
//        for(final Use<?> type : types) {
//            if(iter.hasNext()) {
//                final Object v = iter.next();
//                ((Use<Object>)type).serialize(v, out);
//            } else {
//                out.writeByte(Code.NULL.ordinal());
//            }
//        }
//    }
//
//    public static Tuple deserializeValue(final DataInput in) throws IOException {
//
//        final List<Object> result = new ArrayList<>();
//        final int size = in.readUnsignedShort();
//        for(int i = 0; i != size; ++i) {
//            result.add(Use.deserialize(in));
//        }
//        return new Tuple(result);
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public Tuple expand(final Tuple value, final Expander expander, final Set<Path> expand) {
//
//        if(value != null) {
//            boolean changed = false;
//            final List<Object> expanded = new ArrayList<>();
//            final Iterator iter = value.iterator();
//            for(final Use<?> type : types) {
//                final Object before;
//                if (iter.hasNext()) {
//                    before = iter.next();
//                } else {
//                    before = null;
//                }
//                final Object after = ((Use<Object>)type).expand(before, expander, expand);
//                changed = changed || (before != after);
//            }
//            return changed ? new Tuple(expanded) : value;
//        } else {
//            return null;
//        }
//    }
//
////    @Override
////    public Map<String, Object> openApiType() {
////
////        return ImmutableMap.of(
////                "type", "array",
////                "oneOf", types.stream()
////                        .map(Use::openApiType)
////                        .distinct().collect(Collectors.toList())
////        );
////    }
//
//    @Override
//    public Set<Path> requireExpand(final Set<Path> paths) {
//
//        return types.stream().flatMap(v -> v.requireExpand(paths).stream())
//                .collect(Collectors.toSet());
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public Multimap<Path, Instance> refs(final Tuple value) {
//
//        final Multimap<Path, Instance> result = HashMultimap.create();
//        if(value != null) {
//            final Iterator iter = value.iterator();
//            for(final Use<?> type : types) {
//                if(iter.hasNext()) {
//                    final Object v = iter.next();
//                    ((Use<Object>)type).refs(v)
//                            .forEach(result::put);
//                } else {
//                    break;
//                }
//            }
//        }
//        return result;
//    }
//}
