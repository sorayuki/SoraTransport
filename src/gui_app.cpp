#include "core.hpp"
#include "detail/gui_runtime.hpp"
#include "detail/windows_helpers.hpp"

#include <FL/Fl.H>
#include <FL/Fl_Box.H>
#include <FL/Fl_Button.H>
#include <FL/Fl_Double_Window.H>
#include <FL/Fl_Group.H>
#include <FL/Fl_Input.H>
#include <FL/Fl_Return_Button.H>
#include <FL/Fl_Scroll.H>
#include <FL/Fl_Tabs.H>
#include <FL/fl_ask.H>
#include <FL/platform.H>

#include <chrono>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace soratransport {

namespace {

constexpr int kStopTransferChoice = 1;

std::string format_size(std::uint64_t bytes) {
	static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
	double value = static_cast<double>(bytes);
	std::size_t unit_index = 0;
	while (value >= 1024.0 && unit_index + 1 < std::size(units)) {
		value /= 1024.0;
		++unit_index;
	}

	std::ostringstream out;
	out << std::fixed << std::setprecision(value >= 100.0 ? 0 : 1) << value << ' ' << units[unit_index];
	return out.str();
}

std::string format_rate(std::uint64_t bytes_per_second) {
	return format_size(bytes_per_second) + "/s";
}

std::string format_average_rate(std::uint64_t bytes, std::chrono::steady_clock::duration elapsed) {
	const auto milliseconds = std::max<std::int64_t>(1, std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
	const auto bytes_per_second = static_cast<std::uint64_t>((bytes * 1000ull) / static_cast<std::uint64_t>(milliseconds));
	return format_rate(bytes_per_second);
}

std::string path_to_ui_text(const std::filesystem::path& path) {
	return path_to_utf8_string(path);
}

std::string translate_status_text(std::string_view status_text) {
	if (status_text == "idle") {
		return "空闲";
	}
	if (status_text == "waiting") {
		return "等待操作";
	}
	if (status_text == "starting receiver") {
		return "正在启动接收端";
	}
	if (status_text == "binding listener") {
		return "正在绑定监听端口";
	}
	if (status_text == "waiting for receiver") {
		return "等待接收端连接";
	}
	if (status_text == "receiver connected") {
		return "接收端已连接，等待拖放";
	}
	if (status_text == "sending") {
		return "正在发送";
	}
	if (status_text == "connecting") {
		return "正在连接";
	}
	if (status_text == "receiving") {
		return "正在接收";
	}
	if (status_text == "send completed") {
		return "发送完成";
	}
	if (status_text == "receive completed") {
		return "接收完成";
	}
	if (status_text == "completed") {
		return "已完成";
	}
	if (status_text == "send failed") {
		return "发送失败";
	}
	if (status_text == "receive failed") {
		return "接收失败";
	}
	if (status_text == "cancelled") {
		return "已取消";
	}
	return std::string(status_text);
}

std::wstring utf8_to_utf16(std::string_view text) {
	if (text.empty()) {
		return {};
	}
	const int size = MultiByteToWideChar(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), nullptr, 0);
	if (size <= 0) {
		return L"";
	}
	std::wstring result(static_cast<std::size_t>(size), L'\0');
	MultiByteToWideChar(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), result.data(), size);
	return result;
}

std::string trim_ascii(std::string value) {
	while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
		value.erase(value.begin());
	}
	while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
		value.pop_back();
	}
	return value;
}

std::vector<std::filesystem::path> parse_dropped_paths(std::string_view raw_text) {
	std::vector<std::filesystem::path> paths;
	std::size_t offset = 0;
	while (offset <= raw_text.size()) {
		const auto next = raw_text.find('\n', offset);
		auto piece = raw_text.substr(offset, next == std::string_view::npos ? raw_text.size() - offset : next - offset);
		if (!piece.empty() && piece.back() == '\r') {
			piece.remove_suffix(1);
		}
		auto text = trim_ascii(std::string(piece));
		if (!text.empty()) {
			paths.emplace_back(std::move(text));
		}
		if (next == std::string_view::npos) {
			break;
		}
		offset = next + 1;
	}
	return paths;
}

class DropTargetBox final : public Fl_Box {
public:
	explicit DropTargetBox(int x, int y, int w, int h)
		: Fl_Box(x, y, w, h, "接收端已连接。\n将文件或文件夹拖到这里开始发送。") {
		box(FL_BORDER_BOX);
		align(FL_ALIGN_CENTER | FL_ALIGN_INSIDE | FL_ALIGN_WRAP);
		labelsize(16);
		labelfont(FL_HELVETICA_BOLD);
		update_visual_state();
	}

	void set_drop_handler(std::function<void(std::vector<std::filesystem::path>)> handler) {
		on_drop_ = std::move(handler);
	}

	void set_accepting(bool value) {
		if (accepting_ == value) {
			return;
		}
		accepting_ = value;
		if (!accepting_) {
			highlighted_ = false;
		}
		update_visual_state();
	}

	void set_message(std::string message) {
		copy_label(message.c_str());
		redraw();
	}

	int handle(int event) override {
		if (!accepting_) {
			return Fl_Box::handle(event);
		}

		switch (event) {
		case FL_DND_ENTER:
		case FL_DND_DRAG:
			highlighted_ = true;
			update_visual_state();
			return 1;
		case FL_DND_LEAVE:
			highlighted_ = false;
			update_visual_state();
			return 1;
		case FL_DND_RELEASE:
			highlighted_ = true;
			update_visual_state();
			return 1;
		case FL_PASTE:
			highlighted_ = false;
			update_visual_state();
			if (on_drop_) {
				auto paths = parse_dropped_paths(std::string_view(Fl::event_text(), static_cast<std::size_t>(Fl::event_length())));
				if (!paths.empty()) {
					on_drop_(std::move(paths));
				}
			}
			return 1;
		default:
			return Fl_Box::handle(event);
		}
	}

private:
	void update_visual_state() {
		if (!accepting_) {
			color(fl_rgb_color(229, 224, 216));
			labelcolor(fl_rgb_color(110, 105, 96));
		} else if (highlighted_) {
			color(fl_rgb_color(212, 232, 255));
			labelcolor(fl_rgb_color(30, 62, 102));
		} else {
			color(fl_rgb_color(239, 235, 228));
			labelcolor(fl_rgb_color(58, 54, 48));
		}
		redraw();
	}

	bool accepting_ = true;
	bool highlighted_ = false;
	std::function<void(std::vector<std::filesystem::path>)> on_drop_;
};

struct ProgressViewState {
	std::chrono::steady_clock::time_point started_at = std::chrono::steady_clock::now();
	std::optional<std::chrono::steady_clock::time_point> completed_at;
	std::chrono::steady_clock::time_point last_sample_time{};
	std::uint64_t last_bytes = 0;
	std::uint64_t last_files = 0;
	std::uint64_t recent_rate = 0;
	std::uint64_t recent_files = 0;
	std::string last_status_text;
};

class AppWindow final : public Fl_Double_Window {
public:
	AppWindow(std::filesystem::path current_dir, std::optional<SoratransUrl> clipboard_url)
		: Fl_Double_Window(980, 620, "SoraTransport 文件传输"),
		  current_dir_(std::move(current_dir)),
		  send_progress_(std::make_shared<TransferProgress>()),
		  receive_progress_(std::make_shared<TransferProgress>()),
		  send_server_(send_progress_),
		  receive_url_(std::move(clipboard_url)) {
		begin();
		title_box_ = new Fl_Box(20, 18, 940, 34, "SoraTransport 文件传输");
		tabs_ = new Fl_Tabs(20, 72, 940, 250);
		build_send_tab();
		build_receive_tab();
		tabs_->end();
		detail_box_ = new Fl_Box(20, 346, 940, 28);
		recent_box_ = new Fl_Box(20, 388, 940, 28);
		total_box_ = new Fl_Box(20, 430, 940, 28);
		status_box_ = new Fl_Box(20, 472, 940, 28);
		end();

		color(fl_rgb_color(247, 244, 238));
		title_box_->labelfont(FL_HELVETICA_BOLD);
		title_box_->labelsize(28);
		for (Fl_Box* box : {detail_box_, recent_box_, total_box_, status_box_}) {
			box->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
			box->labelsize(15);
		}

		drop_target_->set_drop_handler([this](std::vector<std::filesystem::path> paths) {
			handle_drop(std::move(paths));
		});
		connect_button_->callback(connect_callback, this);
		receive_input_->when(FL_WHEN_ENTER_KEY_ALWAYS);
		receive_input_->callback(connect_callback, this);
		tabs_->callback(tabs_changed_callback, this);
		callback(window_close_callback, this);

		if (receive_url_) {
			receive_input_->value(receive_url_->canonical_text.c_str());
			tabs_->value(receive_group_);
		} else {
			tabs_->value(send_group_);
		}

		send_progress_->reset("binding listener");
		receive_progress_->reset("waiting");
		try {
			send_server_.start();
		} catch (const std::exception& error) {
			send_progress_->set_failed(error.what());
		}

		update_ui();
		Fl::add_timeout(0.25, timer_callback, this);
	}

	~AppWindow() override {
		Fl::remove_timeout(timer_callback, this);
		stop_receive_session(true);
		send_server_.stop();
	}

private:
	void build_send_tab() {
		send_group_ = new Fl_Group(22, 96, 936, 224, "发送");
		send_intro_box_ = new Fl_Box(38, 124, 900, 24, "先复制下方链接给接收端。接收端连入后，这里会切换成拖放框。");
		send_intro_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		send_intro_box_->labelsize(15);
		send_address_title_box_ = new Fl_Box(38, 160, 900, 24, "复制发送链接");
		send_address_title_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		send_address_title_box_->labelfont(FL_HELVETICA_BOLD);
		send_address_title_box_->labelsize(16);
		send_address_scroll_ = new Fl_Scroll(38, 194, 900, 106);
		send_empty_box_ = new Fl_Box(50, 204, 876, 88, "正在准备发送地址...");
		send_empty_box_->align(FL_ALIGN_LEFT | FL_ALIGN_WRAP | FL_ALIGN_INSIDE);
		send_empty_box_->labelsize(14);
		drop_target_ = new DropTargetBox(38, 194, 900, 106);
		drop_target_->hide();
		send_group_->end();
	}

	void build_receive_tab() {
		receive_group_ = new Fl_Group(22, 96, 936, 224, "接收");
		receive_intro_box_ = new Fl_Box(38, 124, 900, 24, "输入发送者链接后连接；接收到的数据会保存到当前工作目录。");
		receive_intro_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		receive_intro_box_->labelsize(15);
		receive_destination_box_ = new Fl_Box(38, 160, 900, 24, "保存目录：");
		receive_destination_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		receive_destination_box_->labelsize(15);
		receive_input_ = new Fl_Input(38, 204, 736, 34);
		receive_input_->textsize(15);
		connect_button_ = new Fl_Return_Button(790, 204, 148, 34, "连接");
		connect_button_->color(fl_rgb_color(233, 226, 214));
		connect_button_->selection_color(fl_rgb_color(212, 169, 92));
		receive_hint_box_ = new Fl_Box(38, 252, 900, 46, "支持在输入框按回车，或点击右侧按钮开始连接。");
		receive_hint_box_->align(FL_ALIGN_LEFT | FL_ALIGN_WRAP | FL_ALIGN_INSIDE);
		receive_hint_box_->labelsize(14);
		receive_group_->end();
	}

	static void window_close_callback(Fl_Widget*, void* context) {
		static_cast<AppWindow*>(context)->handle_close_request();
	}

	static void timer_callback(void* context) {
		auto* self = static_cast<AppWindow*>(context);
		self->update_ui();
		Fl::repeat_timeout(0.25, timer_callback, context);
	}

	static void tabs_changed_callback(Fl_Widget*, void* context) {
		static_cast<AppWindow*>(context)->update_ui();
	}

	static void copy_address_callback(Fl_Widget* widget, void* context) {
		auto* self = static_cast<AppWindow*>(context);
		write_clipboard_text(widget->label());
		self->set_transient_status("链接已复制到剪贴板");
	}

	static void connect_callback(Fl_Widget*, void* context) {
		static_cast<AppWindow*>(context)->start_receive_session();
	}

	void set_transient_status(std::string status) {
		transient_status_ = std::move(status);
		transient_status_until_ = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		update_ui();
	}

	void cleanup_finished_receive_session() {
		if (receive_session_finished_.load(std::memory_order_acquire) && receive_thread_.joinable()) {
			receive_thread_.join();
		}
	}

	bool is_receive_session_active() const {
		return receive_thread_.joinable() && !receive_session_finished_.load(std::memory_order_acquire);
	}

	void stop_receive_session(bool join_now) {
		if (receive_cancel_event_) {
			receive_cancel_event_->emit();
		}
		if (receive_thread_.joinable()) {
			receive_thread_.request_stop();
			if (join_now) {
				receive_thread_.join();
				receive_session_finished_.store(true, std::memory_order_release);
			}
		}
	}

	void start_receive_session() {
		cleanup_finished_receive_session();
		if (is_receive_session_active()) {
			set_transient_status("当前接收任务仍在进行");
			return;
		}

		auto parsed_url = parse_soratrans_url(receive_input_->value());
		if (!parsed_url) {
			set_transient_status("请输入有效的 soratrans:// 链接");
			return;
		}

		receive_url_ = *parsed_url;
		receive_progress_->reset("connecting");
		receive_cancel_event_ = std::make_unique<CancelEvent>();
		receive_session_finished_.store(false, std::memory_order_release);
		receive_thread_ = std::jthread([
			this,
			url = *receive_url_,
			cancel_event = receive_cancel_event_.get()](std::stop_token stop_token) {
			try {
				receive_directory(url.host, url.port, current_dir_, receive_progress_, stop_token, cancel_event, true);
			} catch (...) {
			}
			receive_session_finished_.store(true, std::memory_order_release);
			Fl::awake();
		});
		update_ui();
	}

	void handle_drop(std::vector<std::filesystem::path> paths) {
		auto submit_error = send_server_.submit_paths(std::move(paths));
		if (submit_error) {
			set_transient_status(*submit_error);
			return;
		}
		set_transient_status("已开始发送拖放内容");
	}

	void handle_close_request() {
		if (close_requested_) {
			return;
		}

		const auto send_snapshot = send_server_.snapshot();
		const bool active_transfer = send_snapshot.receiver_connected || send_snapshot.transfer_in_progress || is_receive_session_active();
		if (active_transfer) {
			const auto choice = fl_choice("传输或连接仍在进行，要停止吗？", "否", "是", nullptr);
			if (choice != kStopTransferChoice) {
				return;
			}
		}

		close_requested_ = true;
		stop_receive_session(true);
		send_server_.stop();
		hide();
	}

	void rebuild_address_buttons() {
		for (auto* button : address_buttons_) {
			send_address_scroll_->remove(button);
			delete button;
		}
		address_buttons_.clear();

		int y = 8;
		for (const auto& address : addresses_) {
			auto* button = new Fl_Button(send_address_scroll_->x() + 8, send_address_scroll_->y() + y, send_address_scroll_->w() - 24, 34, address.url.c_str());
			button->callback(copy_address_callback, this);
			button->color(fl_rgb_color(233, 226, 214));
			button->selection_color(fl_rgb_color(212, 169, 92));
			send_address_scroll_->add(button);
			address_buttons_.push_back(button);
			y += 42;
		}
		send_address_scroll_->redraw();
	}

	void update_send_addresses(std::uint16_t port) {
		if (port == 0 || address_port_ == port) {
			return;
		}
		address_port_ = port;
		try {
			addresses_ = enumerate_shareable_addresses(port);
			rebuild_address_buttons();
		} catch (const std::exception& error) {
			send_progress_->set_failed(error.what());
		}
	}

	void update_send_controls(const GuiSendServerSnapshot& snapshot) {
		if (snapshot.receiver_connected) {
			send_address_title_box_->hide();
			send_address_scroll_->hide();
			send_empty_box_->hide();
			drop_target_->show();
			if (snapshot.transfer_in_progress) {
				drop_target_->set_accepting(false);
				drop_target_->set_message("正在发送当前拖放内容，请等待本次传输完成。");
			} else {
				drop_target_->set_accepting(true);
				drop_target_->set_message("接收端已连接。\n将文件或文件夹拖到这里开始发送。");
			}
			return;
		}

		drop_target_->hide();
		send_address_title_box_->show();
		if (snapshot.bound_port == 0) {
			send_address_scroll_->hide();
			send_empty_box_->show();
			send_empty_box_->copy_label("正在准备发送地址...");
			return;
		}
		if (addresses_.empty()) {
			send_address_scroll_->hide();
			send_empty_box_->show();
			send_empty_box_->copy_label("暂未找到可分享的非虚拟网卡地址。\n\n如果你刚切换网络，请稍后再试。\n接收端连入后，这里会自动切换为拖放框。");
			return;
		}
		send_empty_box_->hide();
		send_address_scroll_->show();
	}

	void update_receive_controls() {
		receive_destination_box_->copy_label(("保存目录：" + path_to_ui_text(current_dir_)).c_str());
		const bool active = is_receive_session_active();
		if (active) {
			receive_input_->deactivate();
			connect_button_->deactivate();
		} else {
			receive_input_->activate();
			connect_button_->activate();
		}
	}

	void update_progress_view_state(const TransferProgressSnapshot& snapshot, ProgressViewState& state, std::chrono::steady_clock::time_point now) {
		const bool counters_reset = snapshot.processed_bytes < state.last_bytes || snapshot.processed_files < state.last_files;
		const bool phase_reset = snapshot.status_text != state.last_status_text && snapshot.processed_bytes == 0 && snapshot.processed_files == 0;
		if (counters_reset || phase_reset) {
			state.started_at = now;
			state.completed_at.reset();
			state.last_sample_time = now;
			state.last_bytes = snapshot.processed_bytes;
			state.last_files = snapshot.processed_files;
			state.recent_rate = 0;
			state.recent_files = 0;
		}
		if (snapshot.completed && !state.completed_at.has_value()) {
			state.completed_at = now;
		}
		if (!snapshot.completed) {
			state.completed_at.reset();
		}
		if (state.last_sample_time.time_since_epoch().count() == 0) {
			state.last_sample_time = now;
			state.last_bytes = snapshot.processed_bytes;
			state.last_files = snapshot.processed_files;
		}
		const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - state.last_sample_time);
		if (elapsed >= std::chrono::seconds(1)) {
			state.recent_rate = snapshot.processed_bytes - state.last_bytes;
			state.recent_files = snapshot.processed_files - state.last_files;
			state.last_bytes = snapshot.processed_bytes;
			state.last_files = snapshot.processed_files;
			state.last_sample_time = now;
		}
		state.last_status_text = snapshot.status_text;
	}

	void update_detail_box(const GuiSendServerSnapshot& send_snapshot, bool receive_selected) {
		if (receive_selected) {
			auto parsed_url = parse_soratrans_url(receive_input_->value());
			if (parsed_url) {
				detail_box_->copy_label(("来源地址：" + parsed_url->canonical_text + "    保存到：" + path_to_ui_text(current_dir_)).c_str());
			} else {
				detail_box_->copy_label(("保存到：" + path_to_ui_text(current_dir_)).c_str());
			}
			return;
		}

		if (send_snapshot.bound_port == 0) {
			detail_box_->copy_label("发送页：正在绑定监听端口...");
			return;
		}
		if (send_snapshot.receiver_connected) {
			detail_box_->copy_label(("发送页：端口 " + std::to_string(send_snapshot.bound_port) + "    接收端已连入，可等待拖放开始发送").c_str());
			return;
		}
		detail_box_->copy_label(("发送页：端口 " + std::to_string(send_snapshot.bound_port) + "    复制任一链接给接收端").c_str());
	}

	void update_summary_boxes(const TransferProgressSnapshot& snapshot, const ProgressViewState& state, bool receive_selected, std::chrono::steady_clock::time_point now) {
		const auto average_rate_end_time = state.completed_at.value_or(now);
		const std::string io_label = receive_selected ? "磁盘写入" : "磁盘读取";
		recent_box_->copy_label((io_label + "（最近 1 秒）：" + format_rate(state.recent_rate) + "    最近 1 秒文件数：" + std::to_string(state.recent_files)).c_str());
		total_box_->copy_label((io_label + "（累计）：" + format_size(snapshot.processed_bytes) + "    平均速率：" + format_average_rate(snapshot.processed_bytes, average_rate_end_time - state.started_at) + "    累计文件数：" + std::to_string(snapshot.processed_files)).c_str());
	}

	void update_status_box(const TransferProgressSnapshot& snapshot) {
		std::string status_text = translate_status_text(snapshot.status_text);
		if (!transient_status_.empty() && std::chrono::steady_clock::now() >= transient_status_until_) {
			transient_status_.clear();
		}
		if (!transient_status_.empty()) {
			if (!status_text.empty()) {
				status_text += "    ";
			}
			status_text += transient_status_;
		}
		status_box_->copy_label(("状态：" + status_text).c_str());
	}

	void update_ui() {
		cleanup_finished_receive_session();

		const auto now = std::chrono::steady_clock::now();
		const auto send_snapshot = send_server_.snapshot();
		update_send_addresses(send_snapshot.bound_port);
		update_send_controls(send_snapshot);
		update_receive_controls();

		const auto send_progress_snapshot = send_progress_->snapshot();
		const auto receive_progress_snapshot = receive_progress_->snapshot();
		update_progress_view_state(send_progress_snapshot, send_view_state_, now);
		update_progress_view_state(receive_progress_snapshot, receive_view_state_, now);

		const bool receive_selected = tabs_->value() == receive_group_;
		const auto& selected_progress = receive_selected ? receive_progress_snapshot : send_progress_snapshot;
		const auto& selected_view_state = receive_selected ? receive_view_state_ : send_view_state_;
		update_detail_box(send_snapshot, receive_selected);
		update_summary_boxes(selected_progress, selected_view_state, receive_selected, now);
		update_status_box(selected_progress);
	}

	std::filesystem::path current_dir_;
	std::shared_ptr<TransferProgress> send_progress_;
	std::shared_ptr<TransferProgress> receive_progress_;
	GuiSendServer send_server_;
	std::optional<SoratransUrl> receive_url_;
	std::unique_ptr<CancelEvent> receive_cancel_event_;
	std::jthread receive_thread_;
	std::atomic<bool> receive_session_finished_{true};
	std::uint16_t address_port_ = 0;
	std::vector<InterfaceAddress> addresses_;
	std::vector<Fl_Button*> address_buttons_;
	ProgressViewState send_view_state_;
	ProgressViewState receive_view_state_;
	bool close_requested_ = false;
	std::string transient_status_;
	std::chrono::steady_clock::time_point transient_status_until_{};

	Fl_Box* title_box_ = nullptr;
	Fl_Tabs* tabs_ = nullptr;
	Fl_Group* send_group_ = nullptr;
	Fl_Group* receive_group_ = nullptr;
	Fl_Box* send_intro_box_ = nullptr;
	Fl_Box* send_address_title_box_ = nullptr;
	Fl_Scroll* send_address_scroll_ = nullptr;
	Fl_Box* send_empty_box_ = nullptr;
	DropTargetBox* drop_target_ = nullptr;
	Fl_Box* receive_intro_box_ = nullptr;
	Fl_Box* receive_destination_box_ = nullptr;
	Fl_Input* receive_input_ = nullptr;
	Fl_Return_Button* connect_button_ = nullptr;
	Fl_Box* receive_hint_box_ = nullptr;
	Fl_Box* detail_box_ = nullptr;
	Fl_Box* recent_box_ = nullptr;
	Fl_Box* total_box_ = nullptr;
	Fl_Box* status_box_ = nullptr;
};

int run_gui_app(int argc, char** argv) {
	const auto current_dir = std::filesystem::current_path();
	std::optional<SoratransUrl> clipboard_url;
	if (const auto clipboard = read_clipboard_text()) {
		clipboard_url = parse_soratrans_url(*clipboard);
	}

	const int fltk_argc = argc > 0 ? 1 : 0;
	AppWindow window(current_dir, clipboard_url);
	window.show(fltk_argc, argv);
	return Fl::run();
}

} // namespace

} // namespace soratransport

#if defined(_WIN32)
int WINAPI wWinMain(HINSTANCE, HINSTANCE, PWSTR, int) {
	try {
		auto args_storage = soratransport::get_utf8_command_line_args();
		auto argv = soratransport::make_argv_view(args_storage);
		return soratransport::run_gui_app(static_cast<int>(argv.size()), argv.data());
	} catch (const std::exception& error) {
		const auto message = soratransport::utf8_to_utf16(error.what());
		MessageBoxW(nullptr, message.c_str(), L"SoraTransport 错误", MB_ICONERROR | MB_OK);
		return 1;
	}
}
#else
int main(int argc, char** argv) {
	try {
		return soratransport::run_gui_app(argc, argv);
	} catch (const std::exception& error) {
		const auto message = soratransport::utf8_to_utf16(error.what());
		MessageBoxW(nullptr, message.c_str(), L"SoraTransport 错误", MB_ICONERROR | MB_OK);
		return 1;
	}
}
#endif
