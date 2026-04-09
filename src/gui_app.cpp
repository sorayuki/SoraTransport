#include "core.hpp"
#include "detail/windows_helpers.hpp"

#include <FL/Fl.H>
#include <FL/Fl_Box.H>
#include <FL/Fl_Button.H>
#include <FL/Fl_Double_Window.H>
#include <FL/Fl_Scroll.H>
#include <FL/fl_ask.H>
#include <FL/platform.H>

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

namespace soratransport {

namespace {

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

std::string make_label(std::string_view prefix, std::string_view value) {
	std::string label(prefix);
	label += value;
	return label;
}

bool should_mark_transfer_started(std::string_view raw_status_text, const TransferProgressSnapshot& snapshot) {
	return raw_status_text == "receiver connected"
		|| raw_status_text == "send completed"
		|| raw_status_text == "send failed"
		|| snapshot.processed_bytes != 0
		|| snapshot.processed_files != 0;
}

std::string translate_status_text(std::string_view status_text) {
	if (status_text == "idle") {
		return "空闲";
	}
	if (status_text == "waiting") {
		return "等待操作";
	}
	if (status_text == "starting sender") {
		return "正在启动发送端";
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
		return "接收端已连接，正在开始传输";
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

enum class GuiMode {
	Idle,
	Send,
	Receive,
};

class AppWindow final : public Fl_Double_Window {
public:
	AppWindow(GuiMode mode, std::filesystem::path path, std::optional<SoratransUrl> url)
		: Fl_Double_Window(920, 560, "SoraTransport 文件传输"),
		  mode_(mode),
		  path_(std::move(path)),
		  receive_url_(std::move(url)),
		  progress_(std::make_shared<TransferProgress>()),
		  started_at_(std::chrono::steady_clock::now()) {
		begin();
		title_box_ = new Fl_Box(24, 18, 872, 34, "SoraTransport 文件传输");
		mode_box_ = new Fl_Box(24, 66, 872, 28);
		detail_box_ = new Fl_Box(24, 100, 872, 28);
		recent_box_ = new Fl_Box(24, 152, 872, 28);
		total_box_ = new Fl_Box(24, 186, 872, 28);
		status_box_ = new Fl_Box(24, 220, 872, 28);
		address_title_box_ = new Fl_Box(24, 272, 872, 24, "可分享的地址");
		address_scroll_ = new Fl_Scroll(24, 304, 872, 220);
		empty_box_ = new Fl_Box(40, 328, 840, 48, "");
		end();

		color(fl_rgb_color(247, 244, 238));
		title_box_->labelfont(FL_HELVETICA_BOLD);
		title_box_->labelsize(28);
		mode_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		detail_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		recent_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		total_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		status_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		address_title_box_->align(FL_ALIGN_LEFT | FL_ALIGN_INSIDE);
		address_title_box_->labelfont(FL_HELVETICA_BOLD);
		address_title_box_->labelsize(16);
		empty_box_->align(FL_ALIGN_LEFT | FL_ALIGN_WRAP | FL_ALIGN_INSIDE);
		empty_box_->hide();

		for (Fl_Box* box : {mode_box_, detail_box_, recent_box_, total_box_, status_box_}) {
			box->labelsize(15);
		}

		switch (mode_) {
		case GuiMode::Send:
			mode_box_->copy_label("模式：发送");
			detail_box_->copy_label(make_label("源目录：", path_to_ui_text(path_)).c_str());
			address_title_box_->show();
			address_scroll_->show();
			progress_->set_status("starting sender");
			session_thread_ = std::jthread([this](std::stop_token stop_token) {
				try {
					listen_directory(path_, 0, {}, progress_, &bound_port_, stop_token);
				} catch (const std::exception& error) {
					progress_->set_failed(error.what());
				}
				Fl::awake();
			});
			break;
		case GuiMode::Receive:
			mode_box_->copy_label("模式：接收");
			detail_box_->copy_label(make_label("目标目录：", path_to_ui_text(path_)).c_str());
			address_title_box_->hide();
			address_scroll_->hide();
			empty_box_->show();
			empty_box_->copy_label("接收模式会从剪贴板读取 soratrans:// 地址，并将收到的文件保存到当前文件夹。");
			progress_->set_status("starting receiver");
			session_thread_ = std::jthread([this](std::stop_token stop_token) {
				try {
					receive_directory(receive_url_->host, receive_url_->port, path_, progress_, stop_token);
				} catch (const std::exception& error) {
					progress_->set_failed(error.what());
				}
				Fl::awake();
			});
			break;
		case GuiMode::Idle:
			mode_box_->copy_label("模式：待机");
			detail_box_->copy_label("未提供目录参数，且剪贴板中也没有 soratrans:// 地址。");
			address_title_box_->hide();
			address_scroll_->hide();
			empty_box_->show();
			empty_box_->copy_label("启动时传入一个文件夹即可进入发送模式；或者先把 soratrans:// 地址复制到剪贴板，再在无参数启动时于此处接收。\n\n提示：发送模式下可点击下方地址按钮复制分享地址。");
			progress_->set_status("waiting");
			break;
		}

		callback(window_close_callback, this);
		update_ui();
		Fl::add_timeout(0.25, timer_callback, this);
	}

	~AppWindow() override {
		Fl::remove_timeout(timer_callback, this);
	}

private:
	static void window_close_callback(Fl_Widget*, void* context) {
		static_cast<AppWindow*>(context)->handle_close_request();
	}

	static void copy_address_callback(Fl_Widget* widget, void* context) {
		auto* self = static_cast<AppWindow*>(context);
		write_clipboard_text(widget->label());
		self->transient_status_ = "地址已复制到剪贴板";
		self->transient_status_until_ = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		self->update_ui();
	}

	static void timer_callback(void* context) {
		auto* self = static_cast<AppWindow*>(context);
		self->update_ui();
		Fl::repeat_timeout(0.25, timer_callback, context);
	}

	void handle_close_request() {
		const auto snapshot = progress_->snapshot();
		if (mode_ == GuiMode::Idle || snapshot.completed) {
			hide();
			return;
		}

		if (transfer_started_) {
			const auto choice = fl_choice("传输仍在进行，要停止吗？", "否", "是", nullptr);
			if (choice != 1) {
				return;
			}
		}

		session_thread_.request_stop();
		if (session_thread_.joinable()) {
			session_thread_.join();
		}
		hide();
	}

	void rebuild_address_buttons() {
		for (auto* button : address_buttons_) {
			address_scroll_->remove(button);
			delete button;
		}
		address_buttons_.clear();

		int y = 8;
		for (const auto& address : addresses_) {
			auto* button = new Fl_Button(address_scroll_->x() + 8, address_scroll_->y() + y, address_scroll_->w() - 24, 36, address.url.c_str());
			button->callback(copy_address_callback, this);
			button->color(fl_rgb_color(233, 226, 214));
			button->selection_color(fl_rgb_color(212, 169, 92));
			address_scroll_->add(button);
			address_buttons_.push_back(button);
			y += 44;
		}
		if (addresses_.empty()) {
			empty_box_->show();
			empty_box_->copy_label("暂未找到带默认网关的可用非虚拟网卡地址。\n\n如果你刚接入网络或切换了网卡，请稍后重新打开程序。\n点击上方任一地址即可复制到剪贴板。");
		} else {
			empty_box_->hide();
		}
		address_scroll_->redraw();
	}

	void update_send_addresses() {
		const auto port = bound_port_.load(std::memory_order_relaxed);
		if (mode_ != GuiMode::Send || port == 0 || addresses_loaded_) {
			return;
		}

		addresses_loaded_ = true;
		try {
			addresses_ = enumerate_shareable_addresses(port);
			rebuild_address_buttons();
		} catch (const std::exception& error) {
			progress_->set_failed(error.what());
		}
	}

	void update_ui() {
		update_send_addresses();

		const auto snapshot = progress_->snapshot();
		const auto now = std::chrono::steady_clock::now();
		const std::string raw_status_text = snapshot.status_text;
		if (snapshot.completed && !completed_at_.has_value()) {
			completed_at_ = now;
		}
		if (mode_ == GuiMode::Send && !transfer_started_ && should_mark_transfer_started(raw_status_text, snapshot)) {
			transfer_started_ = true;
			address_title_box_->hide();
			address_scroll_->hide();
			empty_box_->hide();
		}

		if (!transient_status_.empty() && now >= transient_status_until_) {
			transient_status_.clear();
		}
		if (last_sample_time_.time_since_epoch().count() == 0) {
			last_sample_time_ = now;
			last_bytes_ = snapshot.processed_bytes;
			last_files_ = snapshot.processed_files;
		}

		const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_sample_time_);
		if (elapsed >= std::chrono::seconds(1)) {
			recent_rate_ = snapshot.processed_bytes - last_bytes_;
			recent_files_ = snapshot.processed_files - last_files_;
			last_bytes_ = snapshot.processed_bytes;
			last_files_ = snapshot.processed_files;
			last_sample_time_ = now;
		}

		if (mode_ == GuiMode::Receive && receive_url_) {
			detail_box_->copy_label(("传输地址：" + receive_url_->canonical_text + "    保存到：" + path_to_ui_text(path_)).c_str());
		} else if (mode_ == GuiMode::Send) {
			const auto bound_port = bound_port_.load(std::memory_order_relaxed);
			if (bound_port != 0) {
				detail_box_->copy_label(("源目录：" + path_to_ui_text(path_) + "    端口：" + std::to_string(bound_port)).c_str());
			}
		}

		const auto average_rate_end_time = completed_at_.value_or(now);
		const std::string io_label = mode_ == GuiMode::Receive ? "磁盘写入" : "磁盘读取";
		recent_box_->copy_label((io_label + "（最近 1 秒）：" + format_rate(recent_rate_) + "    最近 1 秒文件数：" + std::to_string(recent_files_)).c_str());
		total_box_->copy_label((io_label + "（累计）：" + format_size(snapshot.processed_bytes) + "    平均速率：" + format_average_rate(snapshot.processed_bytes, average_rate_end_time - started_at_) + "    累计文件数：" + std::to_string(snapshot.processed_files)).c_str());

		std::string status_text = translate_status_text(raw_status_text);
		if (mode_ == GuiMode::Send && bound_port_.load(std::memory_order_relaxed) != 0 && raw_status_text == "binding listener") {
			status_text = "等待接收端连接";
		}
		if (!transient_status_.empty()) {
			status_text += "    " + transient_status_;
		}
		status_box_->copy_label(("状态：" + status_text).c_str());
	}

	GuiMode mode_;
	std::filesystem::path path_;
	std::optional<SoratransUrl> receive_url_;
	std::shared_ptr<TransferProgress> progress_;
	std::jthread session_thread_;
	std::atomic<std::uint16_t> bound_port_{0};
	std::chrono::steady_clock::time_point started_at_;
	std::optional<std::chrono::steady_clock::time_point> completed_at_;
	bool addresses_loaded_ = false;
	bool transfer_started_ = false;
	std::vector<InterfaceAddress> addresses_;
	std::vector<Fl_Button*> address_buttons_;
	std::chrono::steady_clock::time_point last_sample_time_{};
	std::chrono::steady_clock::time_point transient_status_until_{};
	std::uint64_t last_bytes_ = 0;
	std::uint64_t last_files_ = 0;
	std::uint64_t recent_rate_ = 0;
	std::uint64_t recent_files_ = 0;
	std::string transient_status_;

	Fl_Box* title_box_ = nullptr;
	Fl_Box* mode_box_ = nullptr;
	Fl_Box* detail_box_ = nullptr;
	Fl_Box* recent_box_ = nullptr;
	Fl_Box* total_box_ = nullptr;
	Fl_Box* status_box_ = nullptr;
	Fl_Box* address_title_box_ = nullptr;
	Fl_Box* empty_box_ = nullptr;
	Fl_Scroll* address_scroll_ = nullptr;
};

int run_gui_app(int argc, char** argv) {
	const auto current_dir = std::filesystem::current_path();
	if (argc >= 2) {
		// prevent Fl::run() from processing command line arguments, since we've already processed them
		argc = 1; 
		
		const std::filesystem::path candidate = argv[1];
		if (std::filesystem::is_directory(candidate)) {
			AppWindow window(GuiMode::Send, candidate, std::nullopt);
			window.show(argc, argv);
			return Fl::run();
		}
	}

	if (const auto clipboard = read_clipboard_text()) {
		if (const auto url = parse_soratrans_url(*clipboard)) {
			AppWindow window(GuiMode::Receive, current_dir, *url);
			window.show(argc, argv);
			return Fl::run();
		}
	}

	AppWindow window(GuiMode::Idle, current_dir, std::nullopt);
	window.show(argc, argv);
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
