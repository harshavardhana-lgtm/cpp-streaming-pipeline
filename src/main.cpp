#include "pipeline/broker.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Options {
    std::size_t records = 200000;
    std::size_t payload_bytes = 256;
    std::size_t partitions = 3;
    std::size_t batch_max = 256;
    std::size_t consumers = 4;
    std::size_t stream_mb = 0;
    bool run_mode = true;
};

void print_usage(const char* program) {
    std::cout << "usage: " << program << " [options]\n"
              << "  --records N     records to publish (default 200000)\n"
              << "  --payload N     payload size in bytes (default 256)\n"
              << "  --partitions N  partition count (default 3)\n"
              << "  --batch N       max records per batch (default 256)\n"
              << "  --consumers N   concurrent consumer groups (default 4)\n"
              << "  --stream-mb N   stream N megabytes through the pipeline\n"
              << "  --transform     run the heterogeneous schema transform\n";
}

void transform_demo() {
    const std::string legacy_csv = "id,lat,lon,status\n1,12.9,77.5,active\n2,13.1,77.6,idle\n";
    const std::string pipe_delimited = "id|lat|lon|status\n1|12.9|77.5|active\n";

    std::cout << "\n--- heterogeneous transform ---\n";
    std::cout << "csv  -> " << pipeline::transform_heterogeneous(legacy_csv, ",") << "\n";
    std::cout << "pipe -> " << pipeline::transform_heterogeneous(pipe_delimited, "|")
              << "\n";
}

void stream_demo(const Options& options) {
    const std::size_t total_bytes = options.stream_mb * 1024ull * 1024ull;
    const std::size_t records = total_bytes / (options.payload_bytes == 0 ? 1
                                                                          : options.payload_bytes);

    pipeline::Broker broker(options.partitions, options.batch_max);
    std::vector<pipeline::Consumer> consumers;
    for (std::size_t i = 0; i < options.consumers; ++i) {
        const pipeline::Topic topic = pipeline::all_topics()[i % pipeline::all_topics().size()];
        consumers.emplace_back(broker, topic, "group-" + std::to_string(i),
                               options.batch_max);
    }

    pipeline::Producer producer(broker);
    const pipeline::ProducerStats produced =
        producer.publish_stream(pipeline::all_topics(), records, options.payload_bytes);

    std::uint64_t total_consumed = 0;
    for (auto& consumer : consumers) {
        pipeline::Batch batch;
        while (consumer.poll(batch)) {
            total_consumed += batch.size();
            consumer.ack();
        }
    }

    std::cout << "\n--- throughput ---\n"
              << "streamed:   " << options.stream_mb << " MB\n"
              << "published:  " << produced.published << " records\n"
              << "consumed:   " << total_consumed << " records\n"
              << "producer:   " << std::fixed << std::setprecision(0)
              << produced.rate_per_sec() << " records/s\n"
              << "payload:    " << std::setprecision(2)
              << (static_cast<double>(produced.bytes) / (1024.0 * 1024.0)) << " MB\n"
              << "partitions: " << broker.partition_count() << "\n";
}

void main_demo(const Options& options) {
    pipeline::Broker broker(options.partitions, options.batch_max);

    for (const auto& topic : pipeline::all_topics()) {
        broker.subscribe(topic, "analytics");
    }

    pipeline::Producer producer(broker);
    const pipeline::ProducerStats produced =
        producer.publish_stream(pipeline::all_topics(), options.records,
                                options.payload_bytes);

    std::vector<std::unique_ptr<pipeline::Consumer>> consumers;
    for (const auto& topic : pipeline::all_topics()) {
        consumers.emplace_back(
            new pipeline::Consumer(broker, topic, "analytics", options.batch_max));
    }

    std::uint64_t total = 0;
    std::size_t batch_count = 0;
    std::size_t max_batch = 0;
    for (auto& holder : consumers) {
        pipeline::Consumer& consumer = *holder;
        pipeline::Batch batch;
        while (consumer.poll(batch)) {
            total += batch.size();
            ++batch_count;
            max_batch = std::max(max_batch, batch.size());
            consumer.ack();
        }
    }

    std::cout << "\n--- pipeline ---\n"
              << "published:  " << produced.published << "\n"
              << "consumed:   " << total << "\n"
              << "batches:    " << batch_count << "\n"
              << "max batch:  " << max_batch << "\n"
              << "records:    " << broker.total_records() << " buffered across "
              << broker.partition_count() << " partitions\n"
              << "publish:    " << std::fixed << std::setprecision(0)
              << produced.rate_per_sec() << " records/s\n"
              << "throughput: " << std::setprecision(2)
              << (static_cast<double>(produced.bytes) / (1024.0 * 1024.0))
              << " MB in " << std::setprecision(1) << (produced.elapsed_ms / 1000.0)
              << " s\n";

    if (total != produced.published) {
        std::cout << "WARNING: consumed " << total << " but published "
                  << produced.published << "\n";
    }
}

}

int main(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string flag(argv[i]);
        const bool has_value = i + 1 < argc;
        if (flag == "--help" || flag == "-h") {
            print_usage(argv[0]);
            return 0;
        }
        if (flag == "--records" && has_value) {
            options.records = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--payload" && has_value) {
            options.payload_bytes = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--partitions" && has_value) {
            options.partitions = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--batch" && has_value) {
            options.batch_max = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--consumers" && has_value) {
            options.consumers = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--stream-mb" && has_value) {
            options.stream_mb = static_cast<std::size_t>(std::atoll(argv[++i]));
        } else if (flag == "--transform") {
            options.run_mode = false;
        }
    }

    if (!options.run_mode) {
        transform_demo();
        return 0;
    }

    if (options.stream_mb > 0) {
        stream_demo(options);
        return 0;
    }

    main_demo(options);
    return 0;
}
