package com.lambdaworks.redis;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.lambdaworks.redis.internal.LettuceAssert;
import com.lambdaworks.redis.protocol.ConnectionWatchdog;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.ConcurrentSet;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Base Redis client. This class holds the netty infrastructure, {@link ClientOptions} and the basic connection procedure. This
 * class creates the netty {@link EventLoopGroup}s for NIO ({@link NioEventLoopGroup}) and EPoll (
 * {@link io.netty.channel.epoll.EpollEventLoopGroup}) with a default of {@code Runtime.getRuntime().availableProcessors() * 4}
 * threads. Reuse the instance as much as possible since the {@link EventLoopGroup} instances are expensive and can consume a
 * huge part of your resources, if you create multiple instances.
 * <p>
 * You can set the number of threads per {@link NioEventLoopGroup} by setting the {@code io.netty.eventLoopThreads} system
 * property to a reasonable number of threads.
 * </p>
 *
 * @author Mark Paluch
 * @since 3.0
 */
public abstract class AbstractRedisClient {

    protected static final PooledByteBufAllocator BUF_ALLOCATOR = PooledByteBufAllocator.DEFAULT;
    protected static final InternalLogger logger = InternalLoggerFactory.getInstance(RedisClient.class);

    /**
     * @deprecated use map eventLoopGroups instead.
     */
    @Deprecated
    protected EventLoopGroup eventLoopGroup;
    protected EventExecutorGroup genericWorkerPool;

    protected final Map<Class<? extends EventLoopGroup>, EventLoopGroup> eventLoopGroups = new ConcurrentHashMap<>(2);
    protected final HashedWheelTimer timer;
    protected final ChannelGroup channels;
    protected final ClientResources clientResources;
    protected long timeout = 60;
    protected TimeUnit unit;
    protected ConnectionEvents connectionEvents = new ConnectionEvents();
    protected Set<Closeable> closeableResources = new ConcurrentSet<>();

    protected volatile ClientOptions clientOptions = ClientOptions.builder().build();

    private final boolean sharedResources;
    private final AtomicBoolean shutdown = new AtomicBoolean();

    /**
     * @deprecated use {@link #AbstractRedisClient(ClientResources)}
     */
    @Deprecated
    protected AbstractRedisClient() {
        this(null);
    }

    /**
     * Create a new instance with client resources.
     *
     * @param clientResources the client resources. If {@literal null}, the client will create a new dedicated instance of
     *        client resources and keep track of them.
     */
    protected AbstractRedisClient(ClientResources clientResources) {

        if (clientResources == null) {
            sharedResources = false;
            this.clientResources = DefaultClientResources.create();
        } else {
            sharedResources = true;
            this.clientResources = clientResources;
        }

        unit = TimeUnit.SECONDS;

        genericWorkerPool = this.clientResources.eventExecutorGroup();
        channels = new DefaultChannelGroup(genericWorkerPool.next());
        timer = new HashedWheelTimer();
    }

    /**
     * Set the default timeout for {@link com.lambdaworks.redis.RedisConnection connections} created by this client. The timeout
     * applies to connection attempts and non-blocking commands.
     * 
     * @param timeout Default connection timeout.
     * @param unit Unit of time for the timeout.
     */
    public void setDefaultTimeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    /**
     * Populate connection builder with necessary resources.
     * 
     * @param socketAddressSupplier address supplier for initial connect and re-connect
     * @param connectionBuilder connection builder to configure the connection
     * @param redisURI URI of the redis instance
     */
    protected void connectionBuilder(Supplier<SocketAddress> socketAddressSupplier, ConnectionBuilder connectionBuilder,
            RedisURI redisURI) {

        Bootstrap redisBootstrap = new Bootstrap();
        redisBootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        redisBootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        redisBootstrap.option(ChannelOption.ALLOCATOR, BUF_ALLOCATOR);

        SocketOptions socketOptions = getOptions().getSocketOptions();

        redisBootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                (int) socketOptions.getConnectTimeoutUnit().toMillis(socketOptions.getConnectTimeout()));
        redisBootstrap.option(ChannelOption.SO_KEEPALIVE, socketOptions.isKeepAlive());
        redisBootstrap.option(ChannelOption.TCP_NODELAY, socketOptions.isTcpNoDelay());

        if (redisURI == null) {
            connectionBuilder.timeout(timeout, unit);
        } else {
            connectionBuilder.timeout(redisURI.getTimeout(), redisURI.getUnit());
            connectionBuilder.password(redisURI.getPassword());
        }

        connectionBuilder.bootstrap(redisBootstrap);
        connectionBuilder.channelGroup(channels).connectionEvents(connectionEvents).timer(timer);
        connectionBuilder.socketAddressSupplier(socketAddressSupplier);
    }

    protected void channelType(ConnectionBuilder connectionBuilder, ConnectionPoint connectionPoint) {

        connectionBuilder.bootstrap().group(getEventLoopGroup(connectionPoint));

        if (connectionPoint != null && connectionPoint.getSocket() != null) {
            checkForEpollLibrary();
            connectionBuilder.bootstrap().channel(EpollProvider.epollDomainSocketChannelClass);
        } else {
            connectionBuilder.bootstrap().channel(NioSocketChannel.class);
        }
    }

    private synchronized EventLoopGroup getEventLoopGroup(ConnectionPoint connectionPoint) {

        if ((connectionPoint == null || connectionPoint.getSocket() == null)
                && !eventLoopGroups.containsKey(NioEventLoopGroup.class)) {

            if (eventLoopGroup == null) {
                eventLoopGroup = clientResources.eventLoopGroupProvider().allocate(NioEventLoopGroup.class);
            }

            eventLoopGroups.put(NioEventLoopGroup.class, eventLoopGroup);
        }

        if (connectionPoint != null && connectionPoint.getSocket() != null) {
            checkForEpollLibrary();

            if (!eventLoopGroups.containsKey(EpollProvider.epollEventLoopGroupClass)) {
                EventLoopGroup epl = clientResources.eventLoopGroupProvider().allocate(EpollProvider.epollEventLoopGroupClass);
                eventLoopGroups.put(EpollProvider.epollEventLoopGroupClass, epl);
            }
        }

        if (connectionPoint == null || connectionPoint.getSocket() == null) {
            return eventLoopGroups.get(NioEventLoopGroup.class);
        }

        if (connectionPoint != null && connectionPoint.getSocket() != null) {
            checkForEpollLibrary();
            return eventLoopGroups.get(EpollProvider.epollEventLoopGroupClass);
        }

        throw new IllegalStateException("This should not have happened in a binary decision. Please file a bug.");
    }

    private void checkForEpollLibrary() {
        EpollProvider.checkForEpollLibrary();
    }

    @SuppressWarnings("unchecked")
    protected <K, V, T extends RedisChannelHandler<K, V>> T initializeChannel(ConnectionBuilder connectionBuilder) {

        RedisChannelHandler<?, ?> connection = connectionBuilder.connection();
        SocketAddress redisAddress = connectionBuilder.socketAddress();
        try {

            logger.debug("Connecting to Redis at {}", redisAddress);

            Bootstrap redisBootstrap = connectionBuilder.bootstrap();
            RedisChannelInitializer initializer = connectionBuilder.build();
            redisBootstrap.handler(initializer);
            ChannelFuture connectFuture = redisBootstrap.connect(redisAddress);

            connectFuture.await();

            if (!connectFuture.isSuccess()) {
                if (connectFuture.cause() instanceof Exception) {
                    throw (Exception) connectFuture.cause();
                }
                connectFuture.get();
            }

            try {
                initializer.channelInitialized().get(connectionBuilder.getTimeout(), connectionBuilder.getTimeUnit());
            } catch (TimeoutException e) {
                throw new RedisConnectionException("Could not initialize channel within " + connectionBuilder.getTimeout() + " "
                        + connectionBuilder.getTimeUnit(), e);
            }
            connection.registerCloseables(closeableResources, connection);

            return (T) connection;
        } catch (RedisException e) {
            connectionBuilder.endpoint().initialState();
            throw e;
        } catch (Exception e) {
            connectionBuilder.endpoint().initialState();
            throw new RedisConnectionException("Unable to connect to " + redisAddress, e);
        }
    }

    /**
     * Shutdown this client and close all open connections. The client should be discarded after calling shutdown. The shutdown
     * has 2 secs quiet time and a timeout of 15 secs.
     */
    public void shutdown() {
        shutdown(2, 15, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this client and close all open connections. The client should be discarded after calling shutdown.
     * 
     * @param quietPeriod the quiet period as described in the documentation
     * @param timeout the maximum amount of time to wait until the executor is shutdown regardless if a task was submitted
     *        during the quiet period
     * @param timeUnit the unit of {@code quietPeriod} and {@code timeout}
     */
    public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {

        if (shutdown.compareAndSet(false, true)) {

            timer.stop();

            while (!closeableResources.isEmpty()) {
                Closeable closeableResource = closeableResources.iterator().next();
                try {
                    closeableResource.close();
                } catch (Exception e) {
                    logger.debug("Exception on Close: " + e.getMessage(), e);
                }
                closeableResources.remove(closeableResource);
            }

            List<Future<?>> closeFutures = new ArrayList<>();

            for (Channel c : channels) {
                ChannelPipeline pipeline = c.pipeline();

                ConnectionWatchdog commandHandler = pipeline.get(ConnectionWatchdog.class);
                if (commandHandler != null) {
                    commandHandler.setListenOnChannelInactive(false);
                }
            }

            ChannelGroupFuture closeFuture = channels.close();
            closeFutures.add(closeFuture);

            if (!sharedResources) {
                clientResources.shutdown(quietPeriod, timeout, timeUnit);
            } else {
                for (EventLoopGroup eventExecutors : eventLoopGroups.values()) {
                    Future<?> groupCloseFuture = clientResources.eventLoopGroupProvider().release(eventExecutors, quietPeriod,
                            timeout, timeUnit);
                    closeFutures.add(groupCloseFuture);
                }
            }

            for (Future<?> future : closeFutures) {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RedisException(e);
                }
            }
        }
    }

    protected int getResourceCount() {
        return closeableResources.size();
    }

    protected int getChannelCount() {
        return channels.size();
    }

    /**
     * Add a listener for the RedisConnectionState. The listener is notified every time a connect/disconnect/IO exception
     * happens. The listeners are not bound to a specific connection, so every time a connection event happens on any
     * connection, the listener will be notified. The corresponding netty channel handler (async connection) is passed on the
     * event.
     * 
     * @param listener must not be {@literal null}
     */
    public void addListener(RedisConnectionStateListener listener) {
        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.addListener(listener);
    }

    /**
     * Removes a listener.
     * 
     * @param listener must not be {@literal null}
     */
    public void removeListener(RedisConnectionStateListener listener) {

        LettuceAssert.notNull(listener, "RedisConnectionStateListener must not be null");
        connectionEvents.removeListener(listener);
    }

    /**
     * Returns the {@link ClientOptions} which are valid for that client. Connections inherit the current options at the moment
     * the connection is created. Changes to options will not affect existing connections.
     * 
     * @return the {@link ClientOptions} for this client
     */
    public ClientOptions getOptions() {
        return clientOptions;
    }

    /**
     * Set the {@link ClientOptions} for the client.
     * 
     * @param clientOptions client options for the client and connections that are created after setting the options
     */
    protected void setOptions(ClientOptions clientOptions) {
        LettuceAssert.notNull(clientOptions, "ClientOptions must not be null");
        this.clientOptions = clientOptions;
    }
}
