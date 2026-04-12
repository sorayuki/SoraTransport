#pragma once

#include "infra.hpp"

#include <memory>

namespace soratransport::detail2 {

struct BudgetedChunkOwner {
	std::shared_ptr<uint8_t> buffer;
	SemaphoreCor::Guard budget_guard;
};

inline DataChunk make_budgeted_chunk(
	std::shared_ptr<uint8_t> buffer,
	std::size_t length,
	std::uint64_t offset,
	SemaphoreCor::Guard budget_guard) {
	auto owner = std::make_shared<BudgetedChunkOwner>(BudgetedChunkOwner{
		.buffer = std::move(buffer),
		.budget_guard = std::move(budget_guard),
	});
	return DataChunk{std::shared_ptr<uint8_t>(owner, owner->buffer.get()), length, offset, false};
}

} // namespace soratransport::detail2