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
constexpr std::size_t kTargetInFlightReadBytes = 192 * 1024 * 1024;
constexpr std::size_t kDefaultMaxInFlightWriteOps = 1;

std::size_t hardware_threads() {
	const auto detected = std::thread::hardware_concurrency();
	return detected == 0 ? 4 : static_cast<std::size_t>(detected);
}

} // namespace

RuntimeConfig make_runtime_config(RuntimeOptions options) {
	const auto threads = hardware_threads();
	RuntimeConfig config;
	config.scanner_threads = std::clamp<std::size_t>(threads / 2, 4, 12);
	config.reader_threads = std::clamp<std::size_t>(threads, 6, 24);
	config.compression_threads = std::clamp<std::size_t>(threads / 2, 2, 8);
	config.read_concurrency = std::clamp<std::size_t>(config.reader_threads * 2, 8, 48);
	config.tar_queue_depth = std::clamp<std::size_t>(config.read_concurrency, 16, 24);
	config.max_in_flight_read_bytes = options.max_in_flight_read_bytes.value_or(kTargetInFlightReadBytes);
	config.max_in_flight_write_ops = std::max<std::size_t>(1, options.max_in_flight_write_ops.value_or(kDefaultMaxInFlightWriteOps));
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

	const auto worker_result = ZSTD_CCtx_setParameter(context, ZSTD_c_nbWorkers, static_cast<int>(worker_count));
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

RuntimeExecutors::RuntimeExecutors(std::size_t scanner_threads, std::size_t reader_threads, std::size_t compression_threads)
	: scanner_threads_(std::max<std::size_t>(1, scanner_threads)),
	  compression_threads_(std::max<std::size_t>(1, compression_threads)),
	  reader_pool_(std::max<std::size_t>(1, reader_threads)),
	  compression_pool_(std::max<std::size_t>(1, compression_threads)) {}

RuntimeExecutors::~RuntimeExecutors() {
	reader_pool_.join();
	compression_pool_.join();
}

std::size_t RuntimeExecutors::scanner_threads() const {
	return scanner_threads_;
}

std::size_t RuntimeExecutors::compression_threads() const {
	return compression_threads_;
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
