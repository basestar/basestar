package io.basestar.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.In;
import io.basestar.expression.logical.Or;
import io.basestar.schema.*;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseString;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class StorageSubscriptions implements Subscriptions {

    private static final int UNSUBSCRIBE_PAGE_SIZE = 50;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String SCHEMA_NAME;

    private static final Namespace NAMESPACE;

    private static final ObjectSchema SCHEMA;

    static {

        SCHEMA_NAME = "Subscription";

        final Index.Builder subIndex = Index.builder()
                .setConsistency(Consistency.ATOMIC)
                .setPartition(ImmutableList.of(Name.of("sub")))
                .setSort(ImmutableList.of(Sort.asc(Name.of("channel"))));

        final Index.Builder keysIndex = Index.builder()
                .setConsistency(Consistency.ATOMIC)
                .setOver(ImmutableMap.of("key", Name.of("keys")))
                .setPartition(ImmutableList.of(Name.of("key")));

        final ObjectSchema.Builder schema = ObjectSchema.builder()
                .setProperty("sub", Property.builder().setType(UseString.DEFAULT))
                .setProperty("channel", Property.builder().setType(UseString.DEFAULT))
                .setProperty("caller", Property.builder().setType(UseString.DEFAULT))
                .setProperty("expression", Property.builder().setType(UseString.DEFAULT))
                .setProperty("info", Property.builder().setType(UseString.DEFAULT))
                .setProperty("keys", Property.builder().setType(new UseArray<>(UseBinary.DEFAULT)))
                .setIndex("sub", subIndex)
                .setIndex("keys", keysIndex);

        NAMESPACE = Namespace.builder()
                .setSchema(Name.of(SCHEMA_NAME), schema)
                .build();

        SCHEMA = NAMESPACE.requireObjectSchema(SCHEMA_NAME);

    }

    private final Storage storage;

    public StorageSubscriptions(final Storage storage) {

        this.storage = storage;
        storage.validate(SCHEMA);
    }

    @Override
    public CompletableFuture<?> subscribe(final Caller caller, final String sub, final String channel, final Set<Subscription.Key> keys, final Expression expression, final SubscriptionInfo info) {

        try {
            final Instant now = Instant.now();
            final Map<String, Object> object = new HashMap<>();

            final String id = id(sub, channel);

            Instance.setSchema(object, Name.of(SCHEMA_NAME));
            Instance.setId(object, id);
            Instance.setCreated(object, now);
            Instance.setUpdated(object, now);
            Instance.setVersion(object, 1L);
            object.put("sub", sub);
            object.put("channel", channel);
            object.put("caller", OBJECT_MAPPER.writeValueAsString(caller));
            object.put("expression", expression.toString());
            object.put("info", info == null ? null : OBJECT_MAPPER.writeValueAsString(info));
            object.put("keys", keys.stream().map(StorageSubscriptions::binaryKey).collect(Collectors.toList()));

            final Instance instance = SCHEMA.create(object);
            final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC, Versioning.CHECKED)
                    .createObject(SCHEMA, id, instance);

            return write.write();

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] binaryKey(final Subscription.Key key) {

        final List<Object> prefix = new ArrayList<>();
        prefix.add(key.getSchema().toString());
        prefix.add(key.getIndex());
        return UseBinary.concat(UseBinary.binaryKey(prefix), key.getPartition());
    }

    private static Expression keyExpression(final Subscription.Key key) {

        return new In(new Constant(StorageSubscriptions.binaryKey(key)), new NameConstant(Name.of("keys")));
    }

    private static String id(final String sub, final String channel) {

        return sub + "_" + channel;
    }

    private static List<Sort> sort() {

        return ImmutableList.of(Sort.asc(ObjectSchema.ID_NAME));
    }

    @Override
    public Pager<Subscription> query(final Set<Subscription.Key> keys) {

        final Expression expression = new Or(keys.stream().map(StorageSubscriptions::keyExpression).toArray(Expression[]::new));

        return storage.query(SCHEMA, expression, sort(), Collections.emptySet()).map(this::fromMap);
    }

    private Subscription fromMap(final Map<String, Object> object) {

        try {

            final Subscription subscription = new Subscription();
            subscription.setSub((String)object.get("sub"));
            subscription.setChannel((String)object.get("channel"));
            subscription.setExpression(Expression.parse((String)object.get("expression")));
            subscription.setCaller(OBJECT_MAPPER.readValue((String)object.get("caller"), Caller.class));
            final String info = (String)object.get("info");
            if(info != null) {
                subscription.setInfo(OBJECT_MAPPER.readValue(info, SubscriptionInfo.class));
            }
            return subscription;

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public CompletableFuture<?> unsubscribe(final String sub, final String channel) {

        final String id = id(sub, channel);
        return storage.get(Consistency.ATOMIC, SCHEMA, id, Collections.emptySet()).thenCompose(before -> {
            final Storage.WriteTransaction write = storage.write(Consistency.ATOMIC, Versioning.CHECKED);
            write.deleteObject(SCHEMA, id(sub, channel), before);
            return write.write();
        });
    }

    @Override
    public CompletableFuture<?> unsubscribeAll(final String sub) {

        final Context context = Context.init(ImmutableMap.of("s", sub));
        final Expression expression = Expression.parseAndBind(context, "sub == s");

        return unsubscribeAll(storage.query(SCHEMA, expression, sort(), Collections.emptySet()), null);
    }

    private CompletableFuture<?> unsubscribeAll(final Pager<Map<String, Object>> pager, final Page.Token token) {

        return pager.page(token, UNSUBSCRIBE_PAGE_SIZE).thenCompose(page -> {

            final Storage.WriteTransaction write = storage.write(Consistency.NONE, Versioning.CHECKED);
            page.forEach(object -> write.deleteObject(SCHEMA, Instance.getId(object), object));

            if(page.hasMore()) {
                return write.write().thenCompose(ignored -> unsubscribeAll(pager, page.getPaging())).thenApply(ignored -> null);
            } else {
                return write.write();
            }
        });
    }
}
