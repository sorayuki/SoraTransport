#pragma once

#include "../core.hpp"

#include <optional>

namespace soratransport {

struct GuiSendServerSnapshot {
	bool listening = false;
	bool receiver_connected = false;
	bool transfer_in_progress = false;
	std::uint16_t bound_port = 0;
};

class GuiSendServer {
public:
	explicit GuiSendServer(const std::shared_ptr<TransferProgress>& progress, RuntimeOptions options = {});
	~GuiSendServer();
	GuiSendServer(const GuiSendServer&) = delete;
	GuiSendServer& operator=(const GuiSendServer&) = delete;

	void start();
	void stop();
	std::optional<std::string> submit_paths(std::vector<std::filesystem::path> paths);
	GuiSendServerSnapshot snapshot() const;

private:
	class State;
	std::unique_ptr<State> state_;
};

} // namespace soratransport