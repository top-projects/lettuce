package com.lambdaworks.redis;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.lambdaworks.redis.internal.LettuceAssert;
import com.lambdaworks.redis.protocol.*;
import com.lambdaworks.redis.resource.ClientResources;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.Timer;

/**
 * Connection builder for connections. This class is part of the internal API.
 * 
 * @author Mark Paluch
 */
public class ConnectionBuilder {

    private Supplier<SocketAddress> socketAddressSupplier;
    private ConnectionEvents connectionEvents;
    private RedisChannelHandler<?, ?> connection;
    private RedisEndpoint endpoint;
    private Supplier<CommandHandler> commandHandlerSupplier;
    private ChannelGroup channelGroup;
    private Timer timer;
    private Bootstrap bootstrap;
    private ClientOptions clientOptions;
    private long timeout;
    private TimeUnit timeUnit;
    private ClientResources clientResources;
    private char[] password;
    private ReconnectionListener reconnectionListener = ReconnectionListener.NO_OP;
    private ConnectionWatchdog connectionWatchdog;

    public static ConnectionBuilder connectionBuilder() {
        return new ConnectionBuilder();
    }

    protected List<ChannelHandler> buildHandlers() {

        LettuceAssert.assertState(channelGroup != null, "ChannelGroup must be set");
        LettuceAssert.assertState(connectionEvents != null, "ConnectionEvents must be set");
        LettuceAssert.assertState(connection != null, "Connection must be set");
        LettuceAssert.assertState(clientResources != null, "ClientResources must be set");
        LettuceAssert.assertState(endpoint != null, "Endpoint must be set");

        List<ChannelHandler> handlers = new ArrayList<>();

        connection.setOptions(clientOptions);

        handlers.add(new ChannelGroupListener(channelGroup));
        handlers.add(new CommandEncoder());
        handlers.add(commandHandlerSupplier.get());

        if (clientOptions.isAutoReconnect()) {
            handlers.add(createConnectionWatchdog());
        } else {
            endpoint.registerConnectionWatchdog(Optional.empty());
        }

        handlers.add(new ConnectionEventTrigger(connectionEvents, connection, clientResources.eventBus()));

        return handlers;
    }

    protected ConnectionWatchdog createConnectionWatchdog() {

        if (connectionWatchdog != null) {
            return connectionWatchdog;
        }

        LettuceAssert.assertState(bootstrap != null, "Bootstrap must be set for autoReconnect=true");
        LettuceAssert.assertState(timer != null, "Timer must be set for autoReconnect=true");
        LettuceAssert.assertState(socketAddressSupplier != null, "SocketAddressSupplier must be set for autoReconnect=true");

        ConnectionWatchdog watchdog = new ConnectionWatchdog(clientResources.reconnectDelay(), clientOptions, bootstrap, timer,
                clientResources.eventExecutorGroup(), socketAddressSupplier, reconnectionListener, connection);

        endpoint.registerConnectionWatchdog(Optional.of(watchdog));

        connectionWatchdog = watchdog;
        return watchdog;
    }

    public RedisChannelInitializer build() {
        return new PlainChannelInitializer(clientOptions.isPingBeforeActivateConnection(), password(), this::buildHandlers,
                clientResources.eventBus());
    }

    public ConnectionBuilder socketAddressSupplier(Supplier<SocketAddress> socketAddressSupplier) {
        this.socketAddressSupplier = socketAddressSupplier;
        return this;
    }

    public SocketAddress socketAddress() {
        LettuceAssert.assertState(socketAddressSupplier != null, "SocketAddressSupplier must be set");
        return socketAddressSupplier.get();
    }

    public ConnectionBuilder timeout(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public ConnectionBuilder reconnectionListener(ReconnectionListener reconnectionListener) {

        LettuceAssert.notNull(reconnectionListener, "ReconnectionListener must not be null");
        this.reconnectionListener = reconnectionListener;
        return this;
    }

    public ConnectionBuilder clientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
        return this;
    }

    public ConnectionBuilder connectionEvents(ConnectionEvents connectionEvents) {
        this.connectionEvents = connectionEvents;
        return this;
    }

    public ConnectionBuilder connection(RedisChannelHandler<?, ?> connection) {
        this.connection = connection;
        return this;
    }

    public ConnectionBuilder channelGroup(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        return this;
    }

    public ConnectionBuilder commandHandler(Supplier<CommandHandler> supplier) {
        this.commandHandlerSupplier = supplier;
        return this;
    }

    public ConnectionBuilder timer(Timer timer) {
        this.timer = timer;
        return this;
    }

    public ConnectionBuilder bootstrap(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        return this;
    }

    public ConnectionBuilder endpoint(RedisEndpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public ConnectionBuilder clientResources(ClientResources clientResources) {
        this.clientResources = clientResources;
        return this;
    }

    public ConnectionBuilder password(char[] password) {
        this.password = password;
        return this;
    }

    public RedisChannelHandler<?, ?> connection() {
        return connection;
    }

    public Bootstrap bootstrap() {
        return bootstrap;
    }

    public ClientOptions clientOptions() {
        return clientOptions;
    }

    public ClientResources clientResources() {
        return clientResources;
    }

    public char[] password() {
        return password;
    }

    public RedisEndpoint endpoint() {
        return endpoint;
    }
}
