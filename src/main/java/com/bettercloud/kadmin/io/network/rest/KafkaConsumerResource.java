package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.KadminConsumerGroupProviderService;
import com.bettercloud.kadmin.io.network.dto.ConsumerInfoModel;
import com.bettercloud.kadmin.io.network.rest.utils.ResponseUtil;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.kadmin.services.BasicKafkaConsumerProviderService;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.bettercloud.util.TimedWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/5/16.
 */
@RestController
@RequestMapping("/api")
public class KafkaConsumerResource {

    private static final Logger LOGGER = LoggerUtils.get(KafkaConsumerResource.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Joiner keyBuilder = Joiner.on(':');
    private static final long IDLE_THRESHOLD = 15L * 60 * 1000; // 15 minutes
    private static final long IDLE_CHECK_DELAY = 60L * 60 * 1000; // 60 minutes

    private final KadminConsumerGroupProviderService<String, Object> kafkaConsumerProvider;
    private final Map<String, TimedWrapper<ConsumerContainer>> kafkaConsumerMap;
    private final DeserializerRegistryService deserializerRegistryService;

    @Autowired
    public KafkaConsumerResource(BasicKafkaConsumerProviderService kafkaConsumerProvider,
                                 DeserializerRegistryService deserializerRegistryService) {
        kafkaConsumerMap = Maps.newConcurrentMap();
        this.kafkaConsumerProvider = kafkaConsumerProvider;
        this.deserializerRegistryService = deserializerRegistryService;
    }

    @Scheduled(fixedRate = IDLE_CHECK_DELAY)
    private void clearMemory() {
        LOGGER.info("Cleaning up connections/memory");
        List<String> keys = kafkaConsumerMap.keySet().stream()
                .filter(k -> kafkaConsumerMap.get(k).getIdleTime() > IDLE_THRESHOLD)
                .collect(Collectors.toList());
        keys.stream()
                .forEach(k -> {
                    TimedWrapper<ConsumerContainer> timedWrapper = kafkaConsumerMap.get(k);
                    ConsumerContainer container = timedWrapper.getData();
                    LOGGER.debug("Disposing old consumer ({}) with timeout {}", k, timedWrapper.getIdleTime());

                    container.getConsumer().shutdown();
                    container.getHandler().clear();
                });
        System.gc();
    }

    @RequestMapping(
            path = "/kafka/read/{topic}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<JsonNode>> readKafka(@PathVariable("topic") String topic,
                                               @RequestParam("since") Optional<Long> oSince,
                                               @RequestParam("window") Optional<Long> oWindow,
                                               @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                               @RequestParam("schemaUrl") Optional<String> schemaUrl,
                                               @RequestParam("size") Optional<Integer> queueSize,
                                               @RequestParam("deserializerId") String deserializerId) {
        String key = keyBuilder.join(kafkaUrl.orElse("default"), schemaUrl.orElse("default"), topic, deserializerId);
        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        if (des == null) {
            return ResponseUtil.error("Invalid deserializer id", HttpStatus.NOT_FOUND);
        }
        if (!kafkaConsumerMap.containsKey(key)) {
            Integer maxSize = queueSize.filter(s -> s < 100).orElse(50);
            QueuedKafkaMessageHandler queue = new QueuedKafkaMessageHandler(maxSize);
            KadminConsumerGroup consumer = kafkaConsumerProvider.get(KadminConsumerConfig.builder()
                            .topic(topic)
                            .kafkaHost(kafkaUrl.orElse(null))
                            .schemaRegistryUrl(schemaUrl.orElse(null))
                            .keyDeserializer(StringDeserializer.class.getName())
                            .valueDeserializer(des)
                            .build(),
                    true);
            consumer.register(queue);
            kafkaConsumerMap.put(key, TimedWrapper.of(ConsumerContainer.builder()
                    .consumer(consumer)
                    .handler(queue)
                    .build()));
        }
        QueuedKafkaMessageHandler handler = kafkaConsumerMap.get(key).getData().getHandler();
        Long since = getSince(oSince, oWindow);
        Page<JsonNode> page = null;
        try {
            List<JsonNode> messages = handler.get(since).stream()
                    .map(m -> (QueuedKafkaMessageHandler.MessageContainer) m)
                    .map(mc -> {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("key", mc.getKey());
                        node.put("writeTime", mc.getWriteTime());
                        node.put("offset", mc.getOffset());
                        node.put("topic", mc.getTopic());
                        JsonNode message = des.getPrepareOutputFunc().apply(mc.getMessage());
                        node.replace("message", message);
                        // disgusting hack end
                        return node;
                    })
                    .map(n -> (JsonNode) n)
                    .collect(Collectors.toList());
            page = new Page<>();
            page.setPage(0);
            page.setSize(messages.size());
            page.setTotalElements(handler.total());
            page.setContent(messages);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseUtil.error(e);
        }
        return ResponseEntity.ok(page);
    }

    @RequestMapping(
            path = "/avro/read/{topic}",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Boolean> clear(@PathVariable("topic") String topic,
                                         @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                         @RequestParam("schemaUrl") Optional<String> schemaUrl) {
        String key = keyBuilder.join(kafkaUrl.orElse("default"), schemaUrl.orElse("default"), topic);
        boolean cleared = false;
        if (kafkaConsumerMap.containsKey(key) && kafkaConsumerMap.get(key) != null) {
            kafkaConsumerMap.get(key).getData().getHandler().clear();
            cleared = true;
        }
        return ResponseEntity.ok(cleared);
    }

    @RequestMapping(
            path = "/avro/read/{topic}/kill",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Boolean> kill(@PathVariable("topic") String topic,
                                         @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                         @RequestParam("schemaUrl") Optional<String> schemaUrl) {
        String key = keyBuilder.join(kafkaUrl.orElse("default"), schemaUrl.orElse("default"), topic);
        boolean cleared = false;
        if (kafkaConsumerMap.containsKey(key) && kafkaConsumerMap.get(key) != null) {
            ConsumerContainer container = kafkaConsumerMap.get(key).getData();
            container.getHandler().clear();
            container.getConsumer().shutdown();
            kafkaConsumerMap.remove(key);
            cleared = true;
        }
        return ResponseEntity.ok(cleared);
    }

    @RequestMapping(
            path = "/manager/consumers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<ConsumerInfoModel>> getAllConsumers() {
        Page<ConsumerInfoModel> consumers = new Page<>();
        List<ConsumerInfoModel> content = kafkaConsumerMap.values().stream()
                .map(e -> {
                    KadminConsumerGroup consumer = e.getData(false).getConsumer();
                    QueuedKafkaMessageHandler handler = e.getData(false).getHandler();
                    Long lastMessageTime = Opt.of(handler.get(-1L))
                            .filter(list -> !list.isEmpty())
                            .map(list -> list.get(0))
                            .map(m -> (QueuedKafkaMessageHandler.MessageContainer) m)
                            .map(mc -> mc.getWriteTime())
                            .orElse(-1L);
                    return ConsumerInfoModel.builder()
                            .consumerGroupId(consumer.getGroupId())
                            .deserializerName(consumer.getConfig().getValueDeserializer().getName())
                            .deserializerId(consumer.getConfig().getValueDeserializer().getId())
                            .lastMessageTime(lastMessageTime)
                            .lastUsedTime(e.getLastUsed())
                            .queueSize(handler.getQueueSize())
                            .topic(consumer.getConfig().getTopic())
                            .total(handler.total())
                            .build();
                })
                .collect(Collectors.toList());
        consumers.setContent(content);
        consumers.setPage(0);
        consumers.setTotalElements(content.size());
        consumers.setTotalElements(content.size());
        return ResponseEntity.ok(consumers);
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }

    @Data
    @Builder
    private static class ConsumerContainer {
        private KadminConsumerGroup consumer;
        private QueuedKafkaMessageHandler handler;
    }
}
