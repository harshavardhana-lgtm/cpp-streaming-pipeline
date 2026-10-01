#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace pipeline {

enum class Topic : std::uint8_t {
    VesselTelemetry = 0,
    PortCongestion = 1,
    CustomsEvents = 2,
    WeatherAlerts = 3,
    RouteUpdates = 4
};

std::string to_string(Topic topic);
bool topic_from_string(const std::string& name, Topic& out);
std::vector<Topic> all_topics();

struct Delivery {
    Topic topic;
    std::uint64_t offset;
    std::int64_t produced_at;
    std::string payload;
};

struct Batch {
    std::vector<Delivery> records;
    bool end_of_partition = false;

    std::size_t size() const;
    std::size_t byte_size() const;
};

class Broker {
public:
    explicit Broker(std::size_t partitions = 3, std::size_t batch_max = 256);
    ~Broker();

    std::size_t partition_for(Topic topic) const;
    std::uint64_t publish(Topic topic, std::string payload);
    bool consume_batch(Topic topic, const std::string& group, std::size_t max_records,
                       Batch& out);
    bool subscribe(Topic topic, const std::string& group);
    void unsubscribe(Topic topic, const std::string& group);
    std::uint64_t committed(Topic topic, const std::string& group) const;
    bool commit(Topic topic, const std::string& group, std::uint64_t offset);
    void rewind(Topic topic, const std::string& group);

    std::size_t total_records() const;
    std::size_t partition_count() const;
    std::uint64_t dropped() const;
    std::uint64_t redelivered() const;

private:
    struct GroupState {
        std::uint64_t committed = 0;
        std::uint64_t next = 0;
    };

    struct Partition {
        std::vector<Delivery> log;
        std::unordered_map<std::string, GroupState> groups;
        std::mutex mutex;
    };

    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::size_t partitions_;
    std::size_t batch_max_;
    std::atomic<std::uint64_t> dropped_;
    std::atomic<std::uint64_t> redelivered_;
};

struct ConsumerStats {
    std::uint64_t consumed = 0;
    std::uint64_t batches = 0;
    std::uint64_t empty_polls = 0;
    std::uint64_t duplicates = 0;
    double busy_ms = 0.0;
};

class Consumer {
public:
    Consumer(Broker& broker, Topic topic, std::string group, std::size_t batch_max);
    bool poll(Batch& out);
    bool ack();
    bool nack();
    ConsumerStats stats() const;
    std::uint64_t position() const;
    bool is_caught_up() const;

private:
    Broker* broker_;
    Topic topic_;
    std::string group_;
    std::size_t batch_max_;
    std::uint64_t pending_offset_;
    bool has_pending_;
    ConsumerStats stats_;
};

struct ProducerStats {
    std::uint64_t published = 0;
    std::size_t bytes = 0;
    double elapsed_ms = 0.0;
    double rate_per_sec() const;
};

class Producer {
public:
    Producer(Broker& broker);
    ProducerStats publish_stream(const std::vector<Topic>& topics, std::size_t count,
                                 std::size_t payload_bytes);
    void stop();

private:
    Broker* broker_;
    std::atomic<bool> running_;
};

std::string generate_payload(std::size_t bytes, std::uint64_t seed);
std::string transform_heterogeneous(const std::string& raw, const char* source_schema);

}
