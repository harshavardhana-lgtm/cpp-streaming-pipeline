#include "pipeline/broker.hpp"

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace {

int failures = 0;

void fail(const std::string& message) {
    std::printf("FAIL %s\n", message.c_str());
    ++failures;
}

void test_offsets_are_monotonic() {
    pipeline::Broker broker(2, 16);
    std::uint64_t previous = 0;
    bool first = true;
    for (std::size_t i = 0; i < 100; ++i) {
        const std::uint64_t offset = broker.publish(pipeline::Topic::VesselTelemetry, "x");
        if (!first && offset <= previous) {
            fail("offsets must increase per partition");
            return;
        }
        previous = offset;
        first = false;
    }
}

void test_batch_respects_limit() {
    pipeline::Broker broker(1, 8);
    for (std::size_t i = 0; i < 100; ++i) {
        broker.publish(pipeline::Topic::CustomsEvents, "payload");
    }

    pipeline::Batch batch;
    int rounds = 0;
    std::size_t seen = 0;
    broker.subscribe(pipeline::Topic::CustomsEvents, "g");
    while (broker.consume_batch(pipeline::Topic::CustomsEvents, "g", 8, batch)) {
        if (batch.size() > 8) {
            fail("batch exceeded configured maximum");
            return;
        }
        seen += batch.size();
        ++rounds;
        if (rounds > 100) {
            break;
        }
    }
    if (seen != 100) {
        fail("expected 100 records across batches, saw " + std::to_string(seen));
    }
}

void test_consumer_requires_ack() {
    pipeline::Broker broker(1, 4);
    broker.publish(pipeline::Topic::RouteUpdates, "a");
    broker.publish(pipeline::Topic::RouteUpdates, "b");

    pipeline::Consumer consumer(broker, pipeline::Topic::RouteUpdates, "g1", 4);
    pipeline::Batch batch;

    if (!consumer.poll(batch)) {
        fail("first poll should return records");
    }
    if (consumer.poll(batch)) {
        fail("unacked batch must not be redelivered");
    }
    if (consumer.position() != 0) {
        fail("position must not advance before ack");
    }
    if (!consumer.ack()) {
        fail("ack should succeed");
    }
    if (consumer.position() != 2) {
        fail("position must advance to 2 after acking offsets 0 and 1");
    }
    if (consumer.ack()) {
        fail("double ack must fail");
    }
}

void test_isolation_between_topics() {
    pipeline::Broker broker(1, 32);
    broker.publish(pipeline::Topic::VesselTelemetry, "telemetry");
    broker.publish(pipeline::Topic::WeatherAlerts, "weather");

    pipeline::Batch batch;
    broker.subscribe(pipeline::Topic::VesselTelemetry, "a");
    if (!broker.consume_batch(pipeline::Topic::VesselTelemetry, "a", 32, batch)) {
        fail("telemetry partition empty");
    }
    if (batch.size() != 1 || batch.records[0].payload != "telemetry") {
        fail("telemetry partition leaked another topic");
    }

    broker.subscribe(pipeline::Topic::WeatherAlerts, "a");
    if (!broker.consume_batch(pipeline::Topic::WeatherAlerts, "a", 32, batch)) {
        fail("weather partition empty");
    }
    if (batch.size() != 1 || batch.records[0].payload != "weather") {
        fail("weather partition leaked another topic");
    }
}

void test_partition_mapping_is_stable() {
    pipeline::Broker broker(4, 8);
    for (int i = 0; i < 50; ++i) {
        if (broker.partition_for(pipeline::Topic::VesselTelemetry) !=
            broker.partition_for(pipeline::Topic::VesselTelemetry)) {
            fail("partition mapping must be stable");
            return;
        }
    }
    if (broker.partition_for(pipeline::Topic::VesselTelemetry) >= 4) {
        fail("partition out of range");
    }
}

void test_group_commit_is_monotonic() {
    pipeline::Broker broker(1, 8);
    broker.subscribe(pipeline::Topic::PortCongestion, "g");
    if (!broker.commit(pipeline::Topic::PortCongestion, "g", 5)) {
        fail("commit should succeed for known group");
    }
    if (broker.committed(pipeline::Topic::PortCongestion, "g") != 6) {
        fail("committed position should be offset+1");
    }
    if (broker.commit(pipeline::Topic::PortCongestion, "g", 3)) {
        fail("regressing commit must be rejected");
    }
    if (broker.committed(pipeline::Topic::PortCongestion, "g") != 6) {
        fail("committed position must not regress");
    }
    if (broker.commit(pipeline::Topic::PortCongestion, "unknown", 1)) {
        fail("commit for unknown group must fail");
    }
}

void test_nack_redelivers() {
    pipeline::Broker broker(1, 8);
    for (std::size_t i = 0; i < 6; ++i) {
        broker.publish(pipeline::Topic::RouteUpdates, "r" + std::to_string(i));
    }

    pipeline::Consumer consumer(broker, pipeline::Topic::RouteUpdates, "g", 2);
    pipeline::Batch first;
    if (!consumer.poll(first)) {
        fail("first poll empty");
        return;
    }
    if (first.records[0].payload != "r0") {
        fail("unexpected first record");
    }
    consumer.nack();

    pipeline::Batch replay;
    if (!consumer.poll(replay)) {
        fail("nacked batch must be redelivered");
        return;
    }
    if (replay.records.empty() || replay.records[0].payload != "r0") {
        fail("redelivery must start from the committed offset");
    }
    if (broker.redelivered() != 1) {
        fail("broker should count the redelivery");
    }
}

void test_no_loss_across_many_records() {
    pipeline::Broker broker(3, 32);
    const std::size_t expected = 5000;
    pipeline::Producer producer(broker);
    producer.publish_stream(pipeline::all_topics(), expected, 16);

    std::uint64_t consumed = 0;
    for (const auto& topic : pipeline::all_topics()) {
        pipeline::Consumer consumer(broker, topic, "verify", 32);
        pipeline::Batch batch;
        while (consumer.poll(batch)) {
            consumed += batch.size();
            consumer.ack();
        }
    }
    if (consumed != expected) {
        fail("lost records: expected " + std::to_string(expected) + " got " +
             std::to_string(consumed));
    }
}

void test_transform() {
    const std::string csv = pipeline::transform_heterogeneous("a,b,c", ",");
    if (csv != "k=a|v1=b|v2=c") {
        fail("csv transform: " + csv);
    }
    const std::string pipe = pipeline::transform_heterogeneous("x|y", "|");
    if (pipe != "k=x|v1=y") {
        fail("pipe transform: " + pipe);
    }
    if (!pipeline::transform_heterogeneous("", ",").empty()) {
        fail("empty input should yield empty output");
    }
}

}

int main() {
    test_offsets_are_monotonic();
    test_batch_respects_limit();
    test_consumer_requires_ack();
    test_isolation_between_topics();
    test_partition_mapping_is_stable();
    test_group_commit_is_monotonic();
    test_nack_redelivers();
    test_no_loss_across_many_records();
    test_transform();

    if (failures == 0) {
        std::printf("all tests passed\n");
        return 0;
    }
    std::printf("%d failure(s)\n", failures);
    return 1;
}
