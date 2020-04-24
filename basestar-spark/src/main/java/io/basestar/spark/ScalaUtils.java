package io.basestar.spark;

import com.google.common.collect.Streams;
import scala.Function1;
import scala.Predef;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.Map$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ScalaUtils {

    public static <K, V> scala.collection.immutable.Map<K, V> scalaEmptyMap() {

        return Map$.MODULE$.empty();
    }

    @SuppressWarnings("unchecked")
    public static <V> Seq<V> scalaEmptySeq() {

        return (Seq<V>)Seq$.MODULE$.empty();
    }

    public static <K, V> scala.collection.immutable.Map.Map1<K, V> scalaSingletonMap(final K k, final V v) {

        return new scala.collection.immutable.Map.Map1<>(k, v);
    }

    public static <K, V> scala.collection.immutable.Map<K, V> asScalaMap(final Map<K, V> map) {

        return JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(Predef.conforms());
    }

    public static <V> Seq<V> asScalaSeq(final Iterable<V> iterable) {

        return asScalaSeq(iterable.iterator());
    }

    public static <V> Seq<V> asScalaSeq(final Iterator<V> iterator) {

        return JavaConverters.asScalaIteratorConverter(iterator).asScala().toSeq();
    }

    public static <V> List<V> asJavaList(final Seq<V> seq) {

        return JavaConverters.seqAsJavaListConverter(seq).asJava();
    }

    public static <K, V> Map<K, V> asJavaMap(final scala.collection.Map<K, V> map) {

        return JavaConverters.mapAsJavaMapConverter(map).asJava();
    }

//    public <T> T[] asArray(final Seq<T> seq, final Class<T> cls) {
//
//        return seq.toArray(classTag(cls));
//    }

    public static <T> ClassTag<T> classTag(final Class<T> cls) {

        return ClassTag$.MODULE$.apply(cls);
    }

    public static <T> Stream<T> asJavaStream(final scala.collection.Iterable<T> iterable) {

        return asJavaStream(iterable.iterator());
    }

    public static <T> Stream<T> asJavaStream(final scala.collection.Iterator<T> iterator) {

        return Streams.stream(JavaConverters.asJavaIteratorConverter(iterator).asJava());
    }

    public static <T, R> Function1<T, R> scalaFunction(final Function<T, R> fn) {

        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(final T v1) {

                return fn.apply(v1);
            }
        };
    }
}
