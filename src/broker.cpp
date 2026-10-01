#include "pipeline/broker.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <sstream>
#include <thread>

namespace pipeline {

namespace {

double now_ms() {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::string group_key(Topic topic, const std::string& group) {
    return std::to_string(static_cast<int>(topic)) + "|" + group;
}

struct TopicNames {
    const char* name;
    Topic topic;
};

const TopicNames kNames[] = {
    {"vessel.telemetry", Topic::VesselTelemetry},
    {"port.congestion", Topic::PortCongestion},
    {"customs.events", Topic::CustomsEvents},
    {"weather.alerts", Topic::WeatherAlerts},
    {"route.updates", Topic::RouteUpdates},
};

}

std::string to_string(Topic topic) {
    for (const auto& entry : kNames) {
        if (entry.topic == topic) {
            return entry.name;
        }
    }
    return "unknown";
}

bool topic_from_string(const std::string& name, Topic& out) {
    for (const auto& entry : kNames) {
        if (name == entry.name) {
            out = entry.topic;
            return true;
        }
    }
    return false;
}

std::vector<Topic> all_topics() {
    std::vector<Topic> topics;
    topics.reserve(sizeof(kNames) / sizeof(kNames[0]));
    for (const auto& entry : kNames) {
        topics.push_back(entry.topic);
    }
    return topics;
}

std::size_t Batch::size() const {
    return records.size();
}

std::size_t Batch::byte_size() const {
    std::size_t total = 0;
    for (const auto& record : records) {
        total += record.payload.size();
    }
    return total;
}

struct Broker::Impl {
    std::vector<std::unique_ptr<Partition>> partitions;
    std::vector<std::uint64_t> next_offset;
    std::vector<std::uint64_t> next_sequence;
};

Broker::Broker(std::size_t partitions, std::size_t batch_max)
    : impl_(new Impl()),
      partitions_(partitions == 0 ? 1 : partitions),
      batch_max_(batch_max == 0 ? 1 : batch_max),
      dropped_(0),
      redelivered_(0) {
    impl_->partitions.reserve(partitions_);
    impl_->next_offset.resize(partitions_, 0);
    impl_->next_sequence.resize(partitions_, 0);
    for (std::size_t i = 0; i < partitions_; ++i) {
        impl_->partitions.push_back(std::unique_ptr<Partition>(new Partition()));
    }
}

Broker::~Broker() = default;

std::size_t Broker::partition_for(Topic topic) const {
    return static_cast<std::size_t>(topic) % partitions_;
}

std::uint64_t Broker::publish(Topic topic, std::string payload) {
    const std::size_t index = partition_for(topic);
    Partition& partition = *impl_->partitions[index];

    std::lock_guard<std::mutex> guard(partition.mutex);
    const std::uint64_t offset = impl_->next_offset[index]++;
    ++impl_->next_sequence[index];

    Delivery delivery;
    delivery.topic = topic;
    delivery.offset = offset;
    delivery.produced_at = static_cast<std::int64_t>(now_ms());
    delivery.payload = std::move(payload);
    partition.log.push_back(std::move(delivery));
    return offset;
}

bool Broker::consume_batch(Topic topic, const std::string& group,
                           std::size_t max_records, Batch& out) {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    const std::size_t limit = max_records == 0 ? batch_max_
                                               : std::min(max_records, batch_max_);

    std::lock_guard<std::mutex> guard(partition.mutex);
    out.records.clear();
    out.end_of_partition = false;

    const std::string key = group_key(topic, group);
    const auto it = partition.groups.find(key);
    if (it == partition.groups.end()) {
        return false;
    }
    GroupState& state = it->second;

    std::size_t scan = state.next;
    while (scan < partition.log.size() && out.records.size() < limit) {
        if (partition.log[scan].topic == topic) {
            out.records.push_back(partition.log[scan]);
        }
        ++scan;
    }
    state.next = scan;
    return !out.records.empty();
}

bool Broker::subscribe(Topic topic, const std::string& group) {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    std::lock_guard<std::mutex> guard(partition.mutex);
    const std::string key = group_key(topic, group);
    if (partition.groups.find(key) != partition.groups.end()) {
        return false;
    }
    partition.groups.emplace(key, GroupState{});
    return true;
}

void Broker::unsubscribe(Topic topic, const std::string& group) {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    std::lock_guard<std::mutex> guard(partition.mutex);
    partition.groups.erase(group_key(topic, group));
}

std::uint64_t Broker::committed(Topic topic, const std::string& group) const {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    std::lock_guard<std::mutex> guard(partition.mutex);
    const auto it = partition.groups.find(group_key(topic, group));
    return it == partition.groups.end() ? 0 : it->second.committed;
}

bool Broker::commit(Topic topic, const std::string& group, std::uint64_t offset) {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    std::lock_guard<std::mutex> guard(partition.mutex);
    const auto it = partition.groups.find(group_key(topic, group));
    if (it == partition.groups.end()) {
        return false;
    }
    const std::uint64_t next_offset = offset + 1;
    if (next_offset < it->second.committed) {
        ++dropped_;
        return false;
    }
    it->second.committed = next_offset;
    return true;
}

void Broker::rewind(Topic topic, const std::string& group) {
    Partition& partition = *impl_->partitions[partition_for(topic)];
    std::lock_guard<std::mutex> guard(partition.mutex);
    const auto it = partition.groups.find(group_key(topic, group));
    if (it == partition.groups.end()) {
        return;
    }
    it->second.next = it->second.committed;
    ++redelivered_;
}

std::uint64_t Broker::redelivered() const {
    return redelivered_.load();
}

std::size_t Broker::total_records() const {
    std::size_t total = 0;
    for (const auto& partition : impl_->partitions) {
        std::lock_guard<std::mutex> guard(partition->mutex);
        total += partition->log.size();
    }
    return total;
}

std::size_t Broker::partition_count() const {
    return partitions_;
}

std::uint64_t Broker::dropped() const {
    return dropped_.load();
}

Consumer::Consumer(Broker& broker, Topic topic, std::string group, std::size_t batch_max)
    : broker_(&broker),
      topic_(topic),
      group_(std::move(group)),
      batch_max_(batch_max == 0 ? 64 : batch_max),
      pending_offset_(0),
      has_pending_(false) {
    broker_->subscribe(topic_, group_);
}

bool Consumer::poll(Batch& out) {
    const double started = now_ms();
    if (has_pending_) {
        ++stats_.duplicates;
        return false;
    }
    if (!broker_->consume_batch(topic_, group_, batch_max_, out)) {
        ++stats_.empty_polls;
        return false;
    }
    if (out.records.empty()) {
        return false;
    }
    pending_offset_ = out.records.back().offset;
    has_pending_ = true;
    stats_.consumed += out.records.size();
    ++stats_.batches;
    stats_.busy_ms += now_ms() - started;
    return true;
}

bool Consumer::ack() {
    if (!has_pending_) {
        return false;
    }
    const bool ok = broker_->commit(topic_, group_, pending_offset_);
    has_pending_ = false;
    return ok;
}

bool Consumer::nack() {
    if (!has_pending_) {
        return false;
    }
    broker_->rewind(topic_, group_);
    has_pending_ = false;
    return true;
}

ConsumerStats Consumer::stats() const {
    return stats_;
}

std::uint64_t Consumer::position() const {
    return broker_->committed(topic_, group_);
}

bool Consumer::is_caught_up() const {
    return !has_pending_;
}

double ProducerStats::rate_per_sec() const {
    if (elapsed_ms <= 0.0) {
        return 0.0;
    }
    return static_cast<double>(published) / (elapsed_ms / 1000.0);
}

Producer::Producer(Broker& broker) : broker_(&broker), running_(true) {}

void Producer::stop() {
    running_ = false;
}

ProducerStats Producer::publish_stream(const std::vector<Topic>& topics, std::size_t count,
                                       std::size_t payload_bytes) {
    ProducerStats stats;
    if (topics.empty()) {
        return stats;
    }
    const double started = now_ms();
    for (std::size_t i = 0; i < count && running_.load(); ++i) {
        const Topic topic = topics[i % topics.size()];
        const std::string payload = generate_payload(payload_bytes, i);
        broker_->publish(topic, payload);
        ++stats.published;
        stats.bytes += payload.size();
    }
    stats.elapsed_ms = now_ms() - started;
    return stats;
}

std::string generate_payload(std::size_t bytes, std::uint64_t seed) {
    static const char kAlphabet[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<std::size_t> pick(
        0, sizeof(kAlphabet) / sizeof(kAlphabet[0]) - 2);

    std::string payload;
    payload.resize(bytes);
    for (std::size_t i = 0; i < bytes; ++i) {
        payload[i] = kAlphabet[pick(rng)];
    }
    return payload;
}

std::string transform_heterogeneous(const std::string& raw, const char* source_schema) {
    if (raw.empty()) {
        return std::string();
    }

    std::istringstream stream(raw);
    std::string line;
    std::ostringstream out;
    bool first = true;

    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        std::vector<std::string> fields;
        std::string field;
        std::istringstream splitter(line);
        while (std::getline(splitter, field, source_schema != nullptr && *source_schema != '\0'
                                                   ? *source_schema
                                                   : ',')) {
            fields.push_back(field);
        }
        if (fields.size() < 2) {
            continue;
        }

        std::string mapped;
        for (std::size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) {
                mapped += '|';
            }
            if (i == 0) {
                mapped += "k=" + fields[i];
            } else {
                mapped += "v" + std::to_string(i) + "=" + fields[i];
            }
        }
        if (!first) {
            out << '\n';
        }
        out << mapped;
        first = false;
    }
    return out.str();
}

}
