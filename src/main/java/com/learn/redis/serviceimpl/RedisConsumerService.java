package com.learn.redis.serviceimpl;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import jakarta.annotation.Nullable;

@Service
public class RedisConsumerService {

    private static final Logger log = LoggerFactory.getLogger(RedisConsumerService.class);

    private static final String STRING_CHANNEL = "string-channel";
    private static final String DTO_CHANNEL = "dto-channel";
    private static final String MAP_CHANNEL = "map-channel";
    private static final String LIST_CHANNEL = "list-channel";

    private final RedisTemplate<String, Object> redisTemplate;

    public RedisConsumerService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    private MessageListener createListener(Consumer<Object> payloadHandler) {
        return (message, pattern) -> {
            try {
                String channel = new String(message.getChannel());
                Object payload = redisTemplate.getValueSerializer().deserialize(message.getBody());
                if (payload == null) {
                    log.warn("Null payload received from channel '{}'", channel);
                    return;
                }
                payloadHandler.accept(payload);
                log.info("Consumed message from channel '{}': {}", channel, payload);
            } catch (Exception e) {
                log.error("Error processing message from channel '{}': {}", new String(message.getChannel()), e.getMessage(), e);
            }
        };
    }

    @Bean
    MessageListener stringListener() {
        return createListener(payload -> {});
    }

    @Bean
    RedisMessageListenerContainer stringContainer(MessageListener stringListener) {
        return createContainer(stringListener, STRING_CHANNEL);
    }

    @Bean
    MessageListener dtoListener() {
        return createListener(payload -> {});
    }

    @Bean
    RedisMessageListenerContainer dtoContainer(MessageListener dtoListener) {
        return createContainer(dtoListener, DTO_CHANNEL);
    }

    @Bean
    MessageListener mapListener() {
        return createListener(payload -> {});
    }

    @Bean
    RedisMessageListenerContainer mapContainer(MessageListener mapListener) {
        return createContainer(mapListener, MAP_CHANNEL);
    }

    @Bean
    MessageListener listListener() {
        return createListener(payload -> {});
    }

    @Bean
    RedisMessageListenerContainer listContainer(MessageListener listListener) {
        return createContainer(listListener, LIST_CHANNEL);
    }

    private RedisMessageListenerContainer createContainer(MessageListener listener, String channel) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        @Nullable var connectionFactory = redisTemplate.getConnectionFactory();
        if (connectionFactory != null) {
            container.setConnectionFactory(connectionFactory);
        } else {
            log.error("ConnectionFactory is null; cannot start container for channel '{}'", channel);
            return null;
        }
        container.addMessageListener(listener, new ChannelTopic(channel));
        return container;
    }
    
}