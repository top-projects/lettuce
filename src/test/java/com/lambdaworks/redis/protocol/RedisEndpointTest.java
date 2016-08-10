package com.lambdaworks.redis.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.codec.Utf8StringCodec;
import com.lambdaworks.redis.internal.LettuceFactories;
import com.lambdaworks.redis.output.StatusOutput;
import com.lambdaworks.redis.resource.ClientResources;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

@RunWith(MockitoJUnitRunner.class)
public class RedisEndpointTest {

    private Queue<RedisCommand<String, String, ?>> q = LettuceFactories.newConcurrentQueue();

    private RedisEndpoint sut;

    private final Command<String, String, String> command = new Command<>(CommandType.APPEND,
            new StatusOutput<String, String>(new Utf8StringCodec()), null);

    @Mock
    private Channel channel;

    @Mock
    private ConnectionFacade connectionFacade;

    @Mock
    private ClientResources clientResources;

    @Mock
    private ConnectionWatchdog connectionWatchdog;

    @BeforeClass
    public static void beforeClass() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(CommandHandler.class.getName());
        loggerConfig.setLevel(Level.ALL);
    }

    @AfterClass
    public static void afterClass() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(CommandHandler.class.getName());
        loggerConfig.setLevel(null);
    }

    @Before
    public void before() throws Exception {

        when(channel.write(any())).thenAnswer(invocation -> {

            if (invocation.getArguments()[0] instanceof RedisCommand) {
                q.add((RedisCommand) invocation.getArguments()[0]);
            }

            if (invocation.getArguments()[0] instanceof Collection) {
                q.addAll((Collection) invocation.getArguments()[0]);
            }

            return new DefaultChannelPromise(channel);
        });

        when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
            if (invocation.getArguments()[0] instanceof RedisCommand) {
                q.add((RedisCommand) invocation.getArguments()[0]);
            }

            if (invocation.getArguments()[0] instanceof Collection) {
                q.addAll((Collection) invocation.getArguments()[0]);
            }
            return new DefaultChannelPromise(channel);
        });

        sut = new RedisEndpoint(ClientOptions.create());
        sut.setConnectionFacade(connectionFacade);
    }

    @Test
    public void writeConnectedShouldWriteCommandToChannel() throws Exception {

        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);

        sut.notifyChannelActive(channel);
        sut.write(command);

        assertThat(sut.getQueue()).isEmpty();
        verify(channel).writeAndFlush(command);
    }

    @Test
    public void writeDisconnectedShouldBufferCommands() throws Exception {

        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);

        sut.write(command);

        assertThat(sut.getQueue()).contains(command);

        verify(channel, never()).writeAndFlush(anyObject(), any());
    }

    @Test
    public void notifyChannelActiveActivatesFacade() throws Exception {

        sut.notifyChannelActive(channel);

        verify(connectionFacade).activated();
    }

    @Test
    public void notifyChannelActiveArmsConnectionWatchdog() throws Exception {

        sut.registerConnectionWatchdog(Optional.of(connectionWatchdog));

        sut.notifyChannelActive(channel);

        verify(connectionWatchdog).arm();
    }

    @Test
    public void notifyChannelInactiveDeactivatesFacade() throws Exception {

        sut.notifyChannelInactive(channel);

        verify(connectionFacade).deactivated();
    }

    @Test
    public void notifyExceptionShouldStoreException() throws Exception {

        sut.notifyException(new IllegalStateException());
        sut.write(command);

        assertThat(command.exception).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void notifyChannelActiveClearsStoredException() throws Exception {

        sut.notifyException(new IllegalStateException());
        sut.notifyChannelActive(channel);
        sut.write(command);

        assertThat(command.exception).isNull();
    }

    @Test
    public void notifyDrainQueuedCommandsShouldBufferCommands() throws Exception {

        Queue<RedisCommand<?, ?, ?>> q = LettuceFactories.newConcurrentQueue();
        q.add(command);

        sut.notifyDrainQueuedCommands(() -> q);

        assertThat(q).isEmpty();
        assertThat(sut.getQueue()).contains(command);
    }

    @Test
    public void notifyDrainQueuedCommandsShouldWriteCommands() throws Exception {

        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);

        Queue<RedisCommand<?, ?, ?>> q = LettuceFactories.newConcurrentQueue();
        q.add(command);

        sut.notifyChannelActive(channel);
        sut.notifyDrainQueuedCommands(() -> q);

        assertThat(q).isEmpty();
        verify(channel).writeAndFlush(eq(Arrays.asList(command)));
    }

    @Test
    public void writeShouldRejectCommandsInDisconnectedState() throws Exception {

        sut = new RedisEndpoint(ClientOptions.builder() //
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS) //
                .build());

        try {
            sut.write(command);
            fail("Missing RedisException");
        } catch (RedisException e) {
            assertThat(e).hasMessageContaining("Commands are rejected");
        }
    }

    @Test
    public void writeShouldRejectCommandsInClosedState() throws Exception {

       sut.close();

        try {
            sut.write(command);
            fail("Missing RedisException");
        } catch (RedisException e) {
            assertThat(e).hasMessageContaining("Connection is closed");
        }
    }

    @Test
    public void writeWithoutAutoReconnectShouldRejectCommandsInDisconnectedState() throws Exception {

        sut = new RedisEndpoint(ClientOptions.builder() //
                .autoReconnect(false) //
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.DEFAULT) //
                .build());

        try {
            sut.write(command);
            fail("Missing RedisException");
        } catch (RedisException e) {
            assertThat(e).hasMessageContaining("Commands are rejected");
        }
    }

    @Test
    public void closeCleansUpResources() throws Exception {

        sut.notifyChannelActive(channel);
        sut.registerConnectionWatchdog(Optional.of(connectionWatchdog));

        sut.close();

        verify(channel).close();
        verify(connectionWatchdog).prepareClose();
    }

    @Test
    public void closeAllowsOnlyOneCall() throws Exception {

        sut.notifyChannelActive(channel);
        sut.registerConnectionWatchdog(Optional.of(connectionWatchdog));

        sut.close();
        sut.close();

        verify(channel).close();
        verify(connectionWatchdog).prepareClose();
    }
}
