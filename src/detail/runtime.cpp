#include "internal.hpp"

#include <algorithm>
#include <malloc.h>
#include <stdexcept>

namespace soratransport {

namespace {

constexpr std::size_t kBufferAlignment = 4 * 1024;
constexpr std::size_t kSmallBufferSize = 64 * 1024;
constexpr std::size_t kMediumBufferSize = 256 * 1024;
constexpr std::size_t kPipelineBufferSize = 1024 * 1024;
constexpr std::size_t kLargePipelineBufferSize = 4 * 1024 * 1024;
constexpr std::size_t kLargeBufferSize = 16 * 1024 * 1024;
constexpr std::size_t kTargetInFlightReadBytes = 96 * 1024 * 1024;
constexpr std::size_t kDefaultTarQueueDepth = 48;
constexpr std::size_t kDefaultMaxInFlightWriteOps = 3;
constexpr std::size_t kDefaultMaxParallelExtractFiles = 48;
constexpr std::size_t kMaxDefaultWorkerThreads = 12;
constexpr std::size_t kOpenConcurrencyMultiplier = 4;
constexpr std::size_t kMaxDefaultOpenConcurrency = 48;
constexpr int kMaxZstdWorkers = 6;

std::size_t hardware_threads() {
	const auto detected = std::thread::hardware_concurrency();
	return detected == 0 ? 4 : static_cast<std::size_t>(detected);
}

} // namespace

RuntimeConfig make_runtime_config(RuntimeOptions options) {
	const auto threads = hardware_threads();
	RuntimeConfig config;
	config.worker_threads = std::clamp<std::size_t>(threads, 1, kMaxDefaultWorkerThreads);
	config.file_open_concurrency = std::clamp<std::size_t>(
		config.worker_threads * kOpenConcurrencyMultiplier,
		static_cast<std::size_t>(1),
		kMaxDefaultOpenConcurrency);
	config.tar_queue_depth = kDefaultTarQueueDepth;
	config.max_in_flight_read_bytes = options.max_in_flight_read_bytes.value_or(kTargetInFlightReadBytes);
	config.max_in_flight_write_bytes = config.max_in_flight_read_bytes;
	config.max_in_flight_write_ops = std::max<std::size_t>(1, options.max_in_flight_write_ops.value_or(kDefaultMaxInFlightWriteOps));
	config.max_parallel_extract_files = std::max<std::size_t>(config.worker_threads, kDefaultMaxParallelExtractFiles);
	return config;
}

void set_zstd_compression_level(ZSTD_CCtx* context, int compression_level) {
	const auto level_result = ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, compression_level);
	if (ZSTD_isError(level_result)) {
		throw std::runtime_error(ZSTD_getErrorName(level_result));
	}
}

void configure_zstd_context(ZSTD_CCtx* context, int compression_level, std::size_t worker_count) {
	set_zstd_compression_level(context, compression_level);

	if (worker_count <= 1) {
		return;
	}

	const auto zstd_workers = static_cast<int>(std::min<std::size_t>(worker_count, kMaxZstdWorkers));
	const auto worker_result = ZSTD_CCtx_setParameter(context, ZSTD_c_nbWorkers, zstd_workers);
	if (ZSTD_isError(worker_result)) {
		const auto error_code = ZSTD_getErrorCode(worker_result);
		if (error_code != ZSTD_error_parameter_unsupported && error_code != ZSTD_error_parameter_outOfBound) {
			throw std::runtime_error(ZSTD_getErrorName(worker_result));
		}
	}
}

void PipelineState::fail(std::exception_ptr error) {
	std::lock_guard lock(mutex_);
	if (!error_) {
		error_ = error;
	}
}

void PipelineState::rethrow_if_failed() const {
	std::lock_guard lock(mutex_);
	if (error_) {
		std::rethrow_exception(error_);
	}
}

RuntimeExecutors::RuntimeExecutors(std::size_t thread_count)
	: thread_count_(std::max<std::size_t>(1, thread_count)),
	  pool_(thread_count_) {}

RuntimeExecutors::~RuntimeExecutors() {
	pool_.join();
}

std::size_t RuntimeExecutors::thread_count() const {
	return thread_count_;
}

boost::asio::any_io_executor RuntimeExecutors::executor() {
	return pool_.get_executor();
}

BufferPool::BufferPool() : buckets_{kSmallBufferSize, kMediumBufferSize, kPipelineBufferSize, kLargePipelineBufferSize, kLargeBufferSize} {}

BufferPool::~BufferPool() {
	for (auto& [bucket_size, free_list] : free_lists_) {
		for (auto* pointer : free_list) {
			_aligned_free(pointer);
		}
	}
}

std::shared_ptr<uint8_t> BufferPool::acquire(std::size_t requested_size) {
	const auto bucket_size = bucket_for(requested_size);
	uint8_t* buffer = nullptr;
	{
		std::lock_guard lock(mutex_);
		auto& free_list = free_lists_[bucket_size];
		if (!free_list.empty()) {
			buffer = free_list.back();
			free_list.pop_back();
		}
	}
	if (buffer == nullptr) {
		buffer = static_cast<uint8_t*>(_aligned_malloc(bucket_size, kBufferAlignment));
		if (buffer == nullptr) {
			throw std::bad_alloc();
		}
	}
	return {buffer, [this, bucket_size](uint8_t* pointer) { recycle(bucket_size, pointer); }};
}

std::size_t BufferPool::bucket_for(std::size_t requested_size) const {
	for (const auto bucket : buckets_) {
		if (requested_size <= bucket) {
			return bucket;
		}
	}
	return requested_size;
}

void BufferPool::recycle(std::size_t bucket_size, uint8_t* buffer) {
	std::lock_guard lock(mutex_);
	free_lists_[bucket_size].push_back(buffer);
}

} // namespace soratransport
