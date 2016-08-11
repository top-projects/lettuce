package com.lambdaworks.redis.cluster;

import static com.lambdaworks.redis.cluster.SlotHash.getSlot;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;
import com.lambdaworks.redis.internal.HostAndPort;
import com.lambdaworks.redis.internal.LettuceAssert;
import com.lambdaworks.redis.protocol.*;

/**
 * Channel writer for cluster operation. This writer looks up the right partition by hash/slot for the operation.
 * 
 * @author Mark Paluch
 * @since 3.0
 */
class ClusterDistributionChannelWriter implements RedisChannelWriter {

    private final RedisChannelWriter defaultWriter;
    private final ClusterEventListener clusterEventListener;
    private final int executionLimit;
    private ClusterConnectionProvider clusterConnectionProvider;
    private boolean closed = false;

    long p20, p21, p22, p23, p24, p25, p26;
    long p30, p31, p32, p33, p34, p35, p36, p37;

    public ClusterDistributionChannelWriter(ClientOptions clientOptions, RedisChannelWriter defaultWriter,
            ClusterEventListener clusterEventListener) {

        this.defaultWriter = defaultWriter;

        if (clientOptions instanceof ClusterClientOptions) {
            this.executionLimit = ((ClusterClientOptions) clientOptions).getMaxRedirects();
        } else {
            this.executionLimit = 5;
        }

        this.clusterEventListener = clusterEventListener;

    }

    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {

        LettuceAssert.notNull(command, "Command must not be null");

        if (closed) {
            throw new RedisException("Connection is closed");
        }

        RedisCommand<K, V, T> commandToSend = command;
        CommandArgs<K, V> args = command.getArgs();

        if (!(command instanceof ClusterCommand)) {
            RedisCommand<K, V, T> singleCommand = command;
            commandToSend = new ClusterCommand<>(singleCommand, this, executionLimit);
        }

        RedisChannelWriter channelWriter = null;

        if (commandToSend instanceof ClusterCommand && !commandToSend.isDone()) {
            ClusterCommand<K, V, T> clusterCommand = (ClusterCommand<K, V, T>) commandToSend;
            if (clusterCommand.isMoved() || clusterCommand.isAsk()) {
                HostAndPort target;
                boolean asking = false;
                if (clusterCommand.isMoved()) {
                    target = getMoveTarget(clusterCommand.getError());
                    clusterEventListener.onMovedRedirection();
                } else {
                    target = getAskTarget(clusterCommand.getError());
                    asking = true;
                    clusterEventListener.onAskRedirection();
                }

                commandToSend.getOutput().setError((String) null);
                RedisChannelHandler<K, V> connection = (RedisChannelHandler<K, V>) clusterConnectionProvider
                        .getConnection(ClusterConnectionProvider.Intent.WRITE, target.getHostText(), target.getPort());
                channelWriter = connection.getChannelWriter();

                if (asking) {
                    // set asking bit
                    StatefulRedisConnection<K, V> statefulRedisConnection = (StatefulRedisConnection<K, V>) connection;
                    statefulRedisConnection.async().asking();
                }
            }
        }

        if (channelWriter == null && args != null && args.getFirstEncodedKey() != null) {
            int hash = getSlot(args.getFirstEncodedKey());
            ClusterConnectionProvider.Intent intent = getIntent(command.getType());

            RedisChannelHandler<K, V> connection = (RedisChannelHandler<K, V>) clusterConnectionProvider.getConnection(intent,
                    hash);

            channelWriter = connection.getChannelWriter();
        }

        if (channelWriter instanceof ClusterDistributionChannelWriter) {
            ClusterDistributionChannelWriter writer = (ClusterDistributionChannelWriter) channelWriter;
            channelWriter = writer.defaultWriter;
        }

        if (command.getOutput() != null) {
            commandToSend.getOutput().setError((String) null);
        }

        if (channelWriter != null && channelWriter != this && channelWriter != defaultWriter) {
            return channelWriter.write(commandToSend);
        }

        defaultWriter.write(commandToSend);

        return command;
    }

    private ClusterConnectionProvider.Intent getIntent(ProtocolKeyword type) {
        for (ProtocolKeyword readOnlyCommand : ReadOnlyCommands.READ_ONLY_COMMANDS) {
            if (readOnlyCommand == type) {
                return ClusterConnectionProvider.Intent.READ;
            }
        }

        return ClusterConnectionProvider.Intent.WRITE;
    }

    static HostAndPort getMoveTarget(String errorMessage) {

        LettuceAssert.notEmpty(errorMessage, "ErrorMessage must not be empty");
        LettuceAssert.isTrue(errorMessage.startsWith(CommandKeyword.MOVED.name()),
                "ErrorMessage must start with " + CommandKeyword.MOVED);

        String[] movedMessageParts = errorMessage.split(" ");
        LettuceAssert.isTrue(movedMessageParts.length >= 3, "ErrorMessage must consist of 3 tokens (" + errorMessage + ")");

        return HostAndPort.parseCompat(movedMessageParts[2]);
    }

    static HostAndPort getAskTarget(String errorMessage) {

        LettuceAssert.notEmpty(errorMessage, "ErrorMessage must not be empty");
        LettuceAssert.isTrue(errorMessage.startsWith(CommandKeyword.ASK.name()),
                "ErrorMessage must start with " + CommandKeyword.ASK);

        String[] movedMessageParts = errorMessage.split(" ");
        LettuceAssert.isTrue(movedMessageParts.length >= 3, "ErrorMessage must consist of 3 tokens (" + errorMessage + ")");

        return HostAndPort.parseCompat(movedMessageParts[2]);
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        closed = true;

        if (defaultWriter != null) {
            defaultWriter.close();
        }

        if (clusterConnectionProvider != null) {
            clusterConnectionProvider.close();
            clusterConnectionProvider = null;
        }
    }

    @Override
    public void setConnectionFacade(ConnectionFacade redisChannelHandler) {
        defaultWriter.setConnectionFacade(redisChannelHandler);
    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        getClusterConnectionProvider().setAutoFlushCommands(autoFlush);
    }

    @Override
    public void flushCommands() {
        getClusterConnectionProvider().flushCommands();
    }

    public ClusterConnectionProvider getClusterConnectionProvider() {
        return clusterConnectionProvider;
    }

    @Override
    public void reset() {
        defaultWriter.reset();
        clusterConnectionProvider.reset();
    }

    public void setClusterConnectionProvider(ClusterConnectionProvider clusterConnectionProvider) {
        this.clusterConnectionProvider = clusterConnectionProvider;
    }

    public void setPartitions(Partitions partitions) {
        clusterConnectionProvider.setPartitions(partitions);
    }

    /**
     * Set from which nodes data is read. The setting is used as default for read operations on this connection. See the
     * documentation for {@link ReadFrom} for more information.
     *
     * @param readFrom the read from setting, must not be {@literal null}
     */
    public void setReadFrom(ReadFrom readFrom) {
        clusterConnectionProvider.setReadFrom(readFrom);
    }

    /**
     * Gets the {@link ReadFrom} setting for this connection. Defaults to {@link ReadFrom#MASTER} if not set.
     * 
     * @return the read from setting
     */
    public ReadFrom getReadFrom() {
        return clusterConnectionProvider.getReadFrom();
    }
}
