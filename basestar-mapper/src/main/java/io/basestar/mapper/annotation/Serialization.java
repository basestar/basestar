package io.basestar.mapper.annotation;

import io.basestar.mapper.serde.Deserializer;
import io.basestar.mapper.serde.Serializer;

public @interface Serialization {

    Class<? extends Serializer> serializer() default Serializer.class;

    Class<? extends Deserializer> deserializer() default Deserializer.class;

    Class<? extends Serializer> keySerializer() default Serializer.class;

    Class<? extends Deserializer> keyDeserializer() default Deserializer.class;

    Class<? extends Serializer> valueSerializer() default Serializer.class;

    Class<? extends Deserializer> valueDeserializer() default Deserializer.class;
}
