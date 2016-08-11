package com.lambdaworks.redis.cluster;

import static com.lambdaworks.redis.protocol.CommandType.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.cluster.api.NodeSelectionSupport;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import com.lambdaworks.redis.cluster.api.rx.RedisAdvancedClusterReactiveCommands;
import com.lambdaworks.redis.cluster.api.sync.NodeSelection;
import com.lambdaworks.redis.cluster.api.sync.NodeSelectionCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.internal.AbstractInvocationHandler;
import com.lambdaworks.redis.internal.LettuceAssert;
import com.lambdaworks.redis.protocol.CompleteableCommand;
import com.lambdaworks.redis.protocol.ConnectionWatchdog;
import com.lambdaworks.redis.protocol.RedisCommand;

import io.netty.channel.ChannelHandler;

/**
 * A thread-safe connection to a Redis Cluster. Multiple threads may share one {@link StatefulRedisClusterConnectionImpl}
 *
 * A {@link ConnectionWatchdog} monitors each connection and reconnects automatically until {@link #close} is called. All
 * pending commands will be (re)sent after successful reconnection.
 * 
 * @author Mark Paluch
 * @since 4.0
 */
public class StatefulRedisClusterConnectionImpl<K, V> extends RedisChannelHandler<K, V>
        implements StatefulRedisClusterConnection<K, V> {

    private Partitions partitions;

    private char[] password;
    private boolean readOnly;

    protected final RedisCodec<K, V> codec;
    protected final RedisAdvancedClusterCommands<K, V> sync;
    protected final RedisAdvancedClusterAsyncCommandsImpl<K, V> async;
    protected final RedisAdvancedClusterReactiveCommandsImpl<K, V> reactive;

    /**
     * Initialize a new connection.
     *
     * @param writer the channel writer
     * @param codec Codec used to encode/decode keys and values.
     * @param timeout Maximum time to wait for a response.
     * @param unit Unit of time for the timeout.
     */
    public StatefulRedisClusterConnectionImpl(RedisChannelWriter writer, RedisCodec<K, V> codec, long timeout,
            TimeUnit unit) {
        super(writer, timeout, unit);
        this.codec = codec;

        this.async = new RedisAdvancedClusterAsyncCommandsImpl<>(this, codec);
        this.sync = (RedisAdvancedClusterCommands) Proxy.newProxyInstance(AbstractRedisClient.class.getClassLoader(),
                new Class<?>[] { RedisAdvancedClusterConnection.class, RedisAdvancedClusterCommands.class }, syncInvocationHandler());
        this.reactive = new RedisAdvancedClusterReactiveCommandsImpl<>(this, codec);
    }

    @Override
    public RedisAdvancedClusterCommands<K, V> sync() {
        return sync;
    }

    public InvocationHandler syncInvocationHandler() {
        return new ClusterFutureSyncInvocationHandler<>(this, async());
    }

    @Override
    public RedisAdvancedClusterAsyncCommands<K, V> async() {
        return async;
    }

    @Override
    public RedisAdvancedClusterReactiveCommands<K, V> reactive() {
        return reactive;
    }

    @Deprecated
    protected RedisAdvancedClusterReactiveCommandsImpl<K, V> getReactiveCommands() {
        return reactive;
    }

    private RedisURI lookup(String nodeId) {

        for (RedisClusterNode partition : partitions) {
            if (partition.getNodeId().equals(nodeId)) {
                return partition.getUri();
            }
        }
        return null;
    }

    @Override
    public StatefulRedisConnection<K, V> getConnection(String nodeId) {

        RedisURI redisURI = lookup(nodeId);

        if (redisURI == null) {
            throw new RedisException("NodeId " + nodeId + " does not belong to the cluster");
        }

        return getClusterDistributionChannelWriter().getClusterConnectionProvider()
                .getConnection(ClusterConnectionProvider.Intent.WRITE, nodeId);
    }

    @Override
    public StatefulRedisConnection<K, V> getConnection(String host, int port) {

        return getClusterDistributionChannelWriter().getClusterConnectionProvider()
                .getConnection(ClusterConnectionProvider.Intent.WRITE, host, port);
    }

    public ClusterDistributionChannelWriter getClusterDistributionChannelWriter() {
        return (ClusterDistributionChannelWriter) super.getChannelWriter();
    }

    @Override
    public void activated() {

        super.activated();
        // do not block in here, since the channel flow will be interrupted.
        if (password != null) {
            async.authAsync(new String(password));
        }

        if (readOnly) {
            async.readOnly();
        }
    }

    @Override
    public <T, C extends RedisCommand<K, V, T>> C dispatch(C cmd) {

        RedisCommand<K, V, T> local = cmd;

        if (local.getType().name().equals(AUTH.name())) {
            local = attachOnComplete(local, status -> {
                if (status.equals("OK") && cmd.getArgs().getFirstString() != null) {
                    this.password = cmd.getArgs().getFirstString().toCharArray();
                }
            });
        }

        if (local.getType().name().equals(READONLY.name())) {
            local = attachOnComplete(local, status -> {
                if (status.equals("OK")) {
                    this.readOnly = true;
                }
            });
        }

        if (local.getType().name().equals(READWRITE.name())) {
            local = attachOnComplete(local, status -> {
                if (status.equals("OK")) {
                    this.readOnly = false;
                }
            });
        }

        return super.dispatch((C) local);
    }

    private <T> RedisCommand<K, V, T> attachOnComplete(RedisCommand<K, V, T> command, Consumer<T> consumer) {

        if (command instanceof CompleteableCommand) {
            CompleteableCommand<T> completeable = (CompleteableCommand<T>) command;
            completeable.onComplete(consumer);
        }
        return command;
    }

    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
        getClusterDistributionChannelWriter().setPartitions(partitions);
    }

    public Partitions getPartitions() {
        return partitions;
    }

    @Override
    public void setReadFrom(ReadFrom readFrom) {
        LettuceAssert.notNull(readFrom, "ReadFrom must not be null");
        getClusterDistributionChannelWriter().setReadFrom(readFrom);
    }

    @Override
    public ReadFrom getReadFrom() {
        return getClusterDistributionChannelWriter().getReadFrom();
    }

    /**
     * Invocation-handler to synchronize API calls which use Futures as backend. This class leverages the need to implement a
     * full sync class which just delegates every request.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @author Mark Paluch
     * @since 3.0
     */
    @SuppressWarnings("unchecked")
    private static class ClusterFutureSyncInvocationHandler<K, V> extends AbstractInvocationHandler {

        private final StatefulRedisClusterConnection<K, V> connection;
        private final Object asyncApi;
        private final Map<Method, Method> apiMethodCache = new ConcurrentHashMap<>(RedisClusterCommands.class.getMethods().length, 1);
        private final Map<Method, Method> connectionMethodCache = new ConcurrentHashMap<>(5, 1);

        private final static Constructor<MethodHandles.Lookup> LOOKUP_CONSTRUCTOR;

        static {
            try {
                LOOKUP_CONSTRUCTOR = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
                if (!LOOKUP_CONSTRUCTOR.isAccessible()) {
                    LOOKUP_CONSTRUCTOR.setAccessible(true);
                }
            } catch (NoSuchMethodException exp) {
                // should be impossible, but...
                throw new IllegalStateException(exp);
            }
        }

        ClusterFutureSyncInvocationHandler(StatefulRedisClusterConnection<K, V> connection, Object asyncApi) {
            this.connection = connection;
            this.asyncApi = asyncApi;
        }

        static MethodHandles.Lookup privateMethodHandleLookup(Class<?> declaringClass) {
            try {
                return LOOKUP_CONSTRUCTOR.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        static MethodHandle getDefaultMethodHandle(Method method) {
            Class<?> declaringClass = method.getDeclaringClass();
            try {
                return privateMethodHandleLookup(declaringClass).unreflectSpecial(method, declaringClass);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Did not pass in an interface method: " + method);
            }
        }

        /**
         *
         * @see AbstractInvocationHandler#handleInvocation(Object, Method, Object[])
         */
        @Override
        protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {

            try {

                if (method.isDefault()) {
                    return getDefaultMethodHandle(method).bindTo(proxy).invokeWithArguments(args);
                }

                if (method.getName().equals("getConnection") && args.length > 0) {
                    Method targetMethod = connectionMethodCache.computeIfAbsent(method, key -> {
                        try {
                            return connection.getClass().getMethod(key.getName(), key.getParameterTypes());
                        } catch (NoSuchMethodException e) {
                            throw new IllegalStateException(e);
                        }
                    });

                    Object result = targetMethod.invoke(connection, args);
                    if (result instanceof StatefulRedisClusterConnection) {
                        StatefulRedisClusterConnection<K, V> connection = (StatefulRedisClusterConnection<K, V>) result;
                        return connection.sync();
                    }

                    if (result instanceof StatefulRedisConnection) {
                        StatefulRedisConnection<K, V> connection = (StatefulRedisConnection<K, V>) result;
                        return connection.sync();
                    }
                }

                if (method.getName().equals("readonly") && args.length == 1) {
                    return nodes((Predicate<RedisClusterNode>) args[0], ClusterConnectionProvider.Intent.READ, false);
                }

                if (method.getName().equals("nodes") && args.length == 1) {
                    return nodes((Predicate<RedisClusterNode>) args[0], ClusterConnectionProvider.Intent.WRITE, false);
                }

                if (method.getName().equals("nodes") && args.length == 2) {
                    return nodes((Predicate<RedisClusterNode>) args[0], ClusterConnectionProvider.Intent.WRITE,
                            (Boolean) args[1]);
                }

                Method targetMethod = apiMethodCache.computeIfAbsent(method, key -> {

                    try {
                        return asyncApi.getClass().getMethod(key.getName(), key.getParameterTypes());
                    } catch (NoSuchMethodException e) {
                        throw new IllegalStateException(e);
                    }
                });

                Object result = targetMethod.invoke(asyncApi, args);

                if (result instanceof RedisFuture) {
                    RedisFuture<?> command = (RedisFuture<?>) result;
                    if (!method.getName().equals("exec") && !method.getName().equals("multi")) {
                        if (connection instanceof StatefulRedisConnection && ((StatefulRedisConnection) connection).isMulti()) {
                            return null;
                        }
                    }
                    return LettuceFutures.awaitOrCancel(command, connection.getTimeout(), connection.getTimeoutUnit());
                }

                return result;

            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        protected NodeSelection<K, V> nodes(Predicate<RedisClusterNode> predicate, ClusterConnectionProvider.Intent intent,
                boolean dynamic) {

            NodeSelectionSupport<RedisCommands<K, V>, ?> selection;

            if (dynamic) {
                selection = new DynamicSyncNodeSelection<>(connection, predicate, intent);
            } else {
                selection = new StaticSyncNodeSelection<>(connection, predicate, intent);
            }

            NodeSelectionInvocationHandler h = new NodeSelectionInvocationHandler((AbstractNodeSelection<?, ?, ?, ?>) selection,
                    true, connection.getTimeout(), connection.getTimeoutUnit());
            return (NodeSelection<K, V>) Proxy.newProxyInstance(NodeSelectionSupport.class.getClassLoader(),
                    new Class<?>[] { NodeSelectionCommands.class, NodeSelection.class }, h);
        }
    }
}
