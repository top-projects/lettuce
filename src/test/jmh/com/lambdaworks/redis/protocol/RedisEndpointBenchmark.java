package com.lambdaworks.redis.protocol;

import com.lambdaworks.redis.cluster.EmptyStatefulRedisConnection;
import org.openjdk.jmh.annotations.*;

import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.output.ValueOutput;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Benchmark for {@link RedisEndpoint}. Test cases:
 * <ul>
 * <li>user command writes</li>
 * </ul>
 * 
 * @author Mark Paluch
 */
@State(Scope.Benchmark)
public class RedisEndpointBenchmark {

    private final static ByteArrayCodec CODEC = new ByteArrayCodec();
    private final static ClientOptions CLIENT_OPTIONS = ClientOptions.create();
    private final static byte[] KEY = "key".getBytes();
    private final static ChannelFuture EMPTY = new EmptyFuture();

    private RedisEndpoint redisEndpoint;
    private Command command;

    @Setup
    public void setup() {

        redisEndpoint = new RedisEndpoint(CLIENT_OPTIONS);
        command = new Command(CommandType.GET, new ValueOutput<>(CODEC), new CommandArgs(CODEC).addKey(KEY));

        redisEndpoint.setConnectionFacade(new EmptyStatefulRedisConnection());
        redisEndpoint.notifyChannelActive(new MyLocalChannel());
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        redisEndpoint.reset();
    }

    @Benchmark
    public void measureUserWrite() {
        redisEndpoint.write(command);
    }

    private final static class MyLocalChannel extends EmbeddedChannel {
        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public ChannelFuture write(Object msg) {
            return EMPTY;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return promise;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return EMPTY;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return promise;
        }
    }

}
