package biz.paluch.redis.extensibility;

import java.util.concurrent.TimeUnit;

import javax.enterprise.inject.Alternative;

import com.lambdaworks.redis.RedisChannelWriter;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.pubsub.PubSubEndpoint;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnectionImpl;

/**
 * Demo code for extending a RedisClient.
 * 
 * @author Mark Paluch
 */
@Alternative
public class MyExtendedRedisClient extends RedisClient {
    public MyExtendedRedisClient() {
    }

    public MyExtendedRedisClient(String host) {
        super(host);
    }

    public MyExtendedRedisClient(String host, int port) {
        super(host, port);
    }

    public MyExtendedRedisClient(RedisURI redisURI) {
        super(redisURI);
    }

    @Override
    protected <K, V> StatefulRedisPubSubConnectionImpl<K, V> newStatefulRedisPubSubConnection(PubSubEndpoint<K, V> endpoint,
            RedisChannelWriter channelWriter, RedisCodec<K, V> codec, long timeout, TimeUnit unit) {
        return new MyPubSubConnection<>(endpoint, channelWriter, codec, timeout, unit);
    }
}
