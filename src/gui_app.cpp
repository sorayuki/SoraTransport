#include "core.hpp"
#include "detail/gui_runtime.hpp"
#include "detail/windows_helpers.hpp"

#include <wx/wx.h>
#include <wx/clipbrd.h>
#include <wx/dnd.h>
#include <wx/notebook.h>
#include <wx/choice.h>
#include <wx/stattext.h>
#include <wx/textctrl.h>
#include <wx/button.h>
#include <wx/sizer.h>
#include <wx/panel.h>
#include <wx/msgdlg.h>
#include <wx/timer.h>
#include <wx/colour.h>

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

constexpr int kTimerId = 1;
constexpr int kTimerIntervalMs = 250;

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

std::wstring path_to_ui_text(const std::filesystem::path& path) {
	return path.wstring();
}

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

// Custom event for thread-safe UI updates from worker threads
wxDECLARE_EVENT(wxEVT_UI_REFRESH, wxThreadEvent);
wxDEFINE_EVENT(wxEVT_UI_REFRESH, wxThreadEvent);

// Drop target for the send panel
class DropTargetPanel : public wxPanel {
public:
	DropTargetPanel(wxWindow* parent, wxWindowID id = wxID_ANY)
		: wxPanel(parent, id, wxDefaultPosition, wxDefaultSize, wxBORDER_SIMPLE) {
		SetDropTarget(new DropTarget(this));

		drop_label_ = new wxStaticText(this, wxID_ANY,
			L"接收端已连接。\n将文件或文件夹拖到这里开始发送。",
			wxDefaultPosition, wxDefaultSize,
			wxALIGN_CENTER_HORIZONTAL | wxST_NO_AUTORESIZE);
		drop_label_->SetFont(drop_label_->GetFont().Bold().Scaled(1.4f));

		auto* sizer = new wxBoxSizer(wxVERTICAL);
		sizer->AddStretchSpacer(1);
		sizer->Add(drop_label_, 0, wxALIGN_CENTER | wxALL, 10);
		sizer->AddStretchSpacer(1);
		SetSizer(sizer);

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
		update_visual_state();
	}

	void set_message(const std::wstring& message) {
		drop_label_->SetLabelText(message);
	}

	bool is_accepting() const {
		return accepting_;
	}

	void fire_drop(std::vector<std::filesystem::path> paths) {
		if (on_drop_) {
			on_drop_(std::move(paths));
		}
	}

private:
	class DropTarget : public wxFileDropTarget {
	public:
		explicit DropTarget(DropTargetPanel* panel) : panel_(panel) {}

		bool OnDropFiles(wxCoord, wxCoord, const wxArrayString& filenames) override {
			if (!panel_->is_accepting()) {
				return false;
			}
			std::vector<std::filesystem::path> paths;
			paths.reserve(filenames.size());
			for (const auto& f : filenames) {
				auto path = std::filesystem::path(f.ToStdWstring());
				if (!path.empty()) {
					paths.push_back(std::move(path));
				}
			}
			if (!paths.empty()) {
				panel_->fire_drop(std::move(paths));
			}
			return true;
		}

	private:
		DropTargetPanel* panel_;
	};

	void update_visual_state() {
		if (!accepting_) {
			SetBackgroundColour(wxColour(229, 224, 216));
			drop_label_->SetForegroundColour(wxColour(110, 105, 96));
		} else {
			SetBackgroundColour(wxColour(239, 235, 228));
			drop_label_->SetForegroundColour(wxColour(58, 54, 48));
		}
		Refresh();
	}

	bool accepting_ = true;
	wxStaticText* drop_label_ = nullptr;
	std::function<void(std::vector<std::filesystem::path>)> on_drop_;
};

class SoraTransportFrame : public wxFrame {
public:
	SoraTransportFrame(std::filesystem::path current_dir, std::optional<SoratransUrl> clipboard_url)
		: wxFrame(nullptr, wxID_ANY, L"SoraTransport 文件传输", wxDefaultPosition, wxSize(980, 620)),
		  current_dir_(std::move(current_dir)),
		  send_progress_(std::make_shared<TransferProgress>()),
		  receive_progress_(std::make_shared<TransferProgress>()),
		  send_server_(send_progress_),
		  receive_url_(std::move(clipboard_url)) {

		SetBackgroundColour(wxColour(247, 244, 238));

		auto* main_panel = new wxPanel(this);
		auto* main_sizer = new wxBoxSizer(wxVERTICAL);

		// Title
		title_label_ = new wxStaticText(main_panel, wxID_ANY, L"SoraTransport 文件传输");
		title_label_->SetFont(title_label_->GetFont().Bold().Scaled(2.0f));
		main_sizer->Add(title_label_, 0, wxALL, 18);

		// Notebook (tabs)
		notebook_ = new wxNotebook(main_panel, wxID_ANY);
		build_send_tab();
		build_receive_tab();
		main_sizer->Add(notebook_, 0, wxEXPAND | wxLEFT | wxRIGHT, 20);

		// Detail line
		detail_label_ = new wxStaticText(main_panel, wxID_ANY, "");
		detail_label_->SetFont(detail_label_->GetFont().Scaled(1.1f));
		main_sizer->Add(detail_label_, 0, wxEXPAND | wxLEFT | wxRIGHT | wxTOP, 20);

		// Stats lines
		recent_label_ = new wxStaticText(main_panel, wxID_ANY, "");
		recent_label_->SetFont(recent_label_->GetFont().Scaled(1.1f));
		main_sizer->Add(recent_label_, 0, wxEXPAND | wxLEFT | wxRIGHT | wxTOP, 8);

		total_label_ = new wxStaticText(main_panel, wxID_ANY, "");
		total_label_->SetFont(total_label_->GetFont().Scaled(1.1f));
		main_sizer->Add(total_label_, 0, wxEXPAND | wxLEFT | wxRIGHT | wxTOP, 8);

		status_label_ = new wxStaticText(main_panel, wxID_ANY, "");
		status_label_->SetFont(status_label_->GetFont().Scaled(1.1f));
		main_sizer->Add(status_label_, 0, wxEXPAND | wxLEFT | wxRIGHT | wxTOP | wxBOTTOM, 8);

		main_panel->SetSizer(main_sizer);

		// Bind events
		notebook_->Bind(wxEVT_NOTEBOOK_PAGE_CHANGED, &SoraTransportFrame::on_tab_changed, this);
		Bind(wxEVT_CLOSE_WINDOW, &SoraTransportFrame::on_close, this);
		Bind(wxEVT_UI_REFRESH, &SoraTransportFrame::on_ui_refresh, this);

		// Timer
		timer_ = new wxTimer(this, kTimerId);
		Bind(wxEVT_TIMER, &SoraTransportFrame::on_timer, this, kTimerId);
		timer_->Start(kTimerIntervalMs);

		// Initial tab selection
		if (receive_url_) {
			receive_input_->SetValue(receive_url_->canonical_text);
			notebook_->SetSelection(1); // receive tab
		} else {
			notebook_->SetSelection(0); // send tab
		}

		send_progress_->reset("binding listener");
		receive_progress_->reset("waiting");
		try {
			send_server_.start();
		} catch (const std::exception& error) {
			send_progress_->set_failed(error.what());
		}

		// 缓存运行时不变的字符串，避免在定时器中反复拼接
		receive_detail_text_ = L"保存到：" + path_to_ui_text(current_dir_);

		update_ui();
	}

	~SoraTransportFrame() override {
		if (timer_) {
			timer_->Stop();
		}
		stop_receive_session(true);
		send_server_.stop();
	}

private:
	void build_send_tab() {
		send_panel_ = new wxPanel(notebook_);
		auto* sizer = new wxBoxSizer(wxVERTICAL);

		// Intro text
		send_intro_label_ = new wxStaticText(send_panel_, wxID_ANY,
			L"先选择并复制下方链接给接收端。接收端连入后，这里会切换成拖放框。");
		send_intro_label_->SetFont(send_intro_label_->GetFont().Scaled(1.1f));
		sizer->Add(send_intro_label_, 0, wxALL, 14);

		// Address title
		send_addr_title_label_ = new wxStaticText(send_panel_, wxID_ANY, L"选择发送链接");
		send_addr_title_label_->SetFont(send_addr_title_label_->GetFont().Bold().Scaled(1.2f));
		sizer->Add(send_addr_title_label_, 0, wxLEFT | wxRIGHT | wxBOTTOM, 14);

		// Address row (choice + copy button)
		auto* addr_sizer = new wxBoxSizer(wxHORIZONTAL);
		send_addr_choice_ = new wxChoice(send_panel_, wxID_ANY);
		send_addr_choice_->SetMinSize(wxSize(600, 34));
		addr_sizer->Add(send_addr_choice_, 1, wxRIGHT, 14);

		copy_button_ = new wxButton(send_panel_, wxID_ANY, L"复制链接");
		copy_button_->SetMinSize(wxSize(142, 36));
		copy_button_->Bind(wxEVT_BUTTON, &SoraTransportFrame::on_copy_address, this);
		addr_sizer->Add(copy_button_, 0);

		sizer->Add(addr_sizer, 0, wxEXPAND | wxLEFT | wxRIGHT | wxBOTTOM, 14);

		// Empty state label
		send_empty_label_ = new wxStaticText(send_panel_, wxID_ANY, L"正在准备发送地址...");
		send_empty_label_->SetFont(send_empty_label_->GetFont().Scaled(1.0f));
		sizer->Add(send_empty_label_, 0, wxLEFT | wxRIGHT | wxBOTTOM, 14);

		// Drop target
		drop_target_ = new DropTargetPanel(send_panel_);
		drop_target_->SetMinSize(wxSize(-1, 106));
		drop_target_->Hide();
		drop_target_->set_drop_handler([this](std::vector<std::filesystem::path> paths) {
			handle_drop(std::move(paths));
		});
		sizer->Add(drop_target_, 1, wxEXPAND | wxLEFT | wxRIGHT | wxBOTTOM, 14);

		send_panel_->SetSizer(sizer);
		notebook_->AddPage(send_panel_, L"发送");
	}

	void build_receive_tab() {
		receive_panel_ = new wxPanel(notebook_);
		auto* sizer = new wxBoxSizer(wxVERTICAL);

		// Intro text
		auto* intro_label = new wxStaticText(receive_panel_, wxID_ANY,
			L"输入发送者链接后连接；接收到的数据会保存到当前工作目录。");
		intro_label->SetFont(intro_label->GetFont().Scaled(1.1f));
		sizer->Add(intro_label, 0, wxALL, 14);

		// Destination directory (current_dir_ 运行时不变，构造时一次性设置)
		receive_dest_label_ = new wxStaticText(receive_panel_, wxID_ANY,
			L"保存目录：" + path_to_ui_text(current_dir_));
		receive_dest_label_->SetFont(receive_dest_label_->GetFont().Scaled(1.1f));
		sizer->Add(receive_dest_label_, 0, wxLEFT | wxRIGHT | wxBOTTOM, 14);

		// Input row
		auto* input_sizer = new wxBoxSizer(wxHORIZONTAL);
		receive_input_ = new wxTextCtrl(receive_panel_, wxID_ANY, L"", wxDefaultPosition, wxDefaultSize, wxTE_PROCESS_ENTER);
		receive_input_->SetMinSize(wxSize(600, 34));
		receive_input_->Bind(wxEVT_TEXT_ENTER, &SoraTransportFrame::on_connect, this);
		input_sizer->Add(receive_input_, 1, wxRIGHT, 14);

		connect_button_ = new wxButton(receive_panel_, wxID_ANY, L"连接");
		connect_button_->SetMinSize(wxSize(148, 34));
		connect_button_->Bind(wxEVT_BUTTON, &SoraTransportFrame::on_connect, this);
		input_sizer->Add(connect_button_, 0);

		sizer->Add(input_sizer, 0, wxEXPAND | wxLEFT | wxRIGHT | wxBOTTOM, 14);

		// Hint
		receive_hint_label_ = new wxStaticText(receive_panel_, wxID_ANY,
			L"支持在输入框按回车，或点击右侧按钮开始连接。");
		receive_hint_label_->SetFont(receive_hint_label_->GetFont().Scaled(1.0f));
		sizer->Add(receive_hint_label_, 0, wxLEFT | wxRIGHT | wxBOTTOM, 14);

		receive_panel_->SetSizer(sizer);
		notebook_->AddPage(receive_panel_, L"接收");
	}

	void on_tab_changed(wxBookCtrlEvent&) {
		update_ui();
	}

	void on_close(wxCloseEvent& event) {
		if (close_requested_) {
			event.Skip();
			return;
		}

		const auto send_snapshot = send_server_.snapshot();
		const auto receive_snapshot = receive_progress_->snapshot();
		const bool active_transfer = send_snapshot.transfer_in_progress
			|| (is_receive_session_active() && receive_snapshot.status_text == "receiving");

		if (active_transfer) {
			auto* dlg = new wxMessageDialog(this,
				L"传输或连接仍在进行，要停止吗？",
				L"确认关闭",
				wxYES_NO | wxNO_DEFAULT | wxICON_QUESTION);
			dlg->SetYesNoLabels(L"是", L"否");
			if (dlg->ShowModal() != wxID_YES) {
				dlg->Destroy();
				return;
			}
			dlg->Destroy();
		}

		close_requested_ = true;
		if (receive_cancel_event_) {
			receive_cancel_event_->emit();
		}
		if (receive_thread_.joinable()) {
			receive_thread_.request_stop();
		}
		event.Skip();
	}

	void on_timer(wxTimerEvent&) {
		update_ui();
	}

	void on_ui_refresh(wxThreadEvent&) {
		update_ui();
	}

	void on_copy_address(wxCommandEvent&) {
		auto selected_url = selected_send_address_url();
		if (!selected_url) {
			set_transient_status(L"当前没有可复制的发送链接");
			return;
		}
		write_clipboard_text(*selected_url);
		set_transient_status(L"链接已复制到剪贴板");
	}

	void on_connect(wxCommandEvent&) {
		start_receive_session();
	}

	void set_transient_status(std::wstring status) {
		transient_status_ = std::move(status);
		transient_status_until_ = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		update_ui();
	}

	void cleanup_finished_receive_session() {
		if (receive_session_finished_.load(std::memory_order_acquire) && receive_thread_.joinable()) {
			receive_thread_.join();
		}
	}

	void maybe_finish_close_request() {
		if (!close_requested_) {
			return;
		}
		cleanup_finished_receive_session();
		if (!receive_thread_.joinable()) {
			Hide();
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
			set_transient_status(L"当前接收任务仍在进行");
			return;
		}

		auto parsed_url = parse_soratrans_url(receive_input_->GetValue().ToStdString());
		if (!parsed_url) {
			set_transient_status(L"请输入有效的 soratrans:// 链接");
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
			// Wake up UI from worker thread
			wxQueueEvent(this, new wxThreadEvent(wxEVT_UI_REFRESH));
		});
		update_ui();
	}

	void handle_drop(std::vector<std::filesystem::path> paths) {
		auto submit_error = send_server_.submit_paths(std::move(paths));
		if (submit_error) {
			set_transient_status(utf8_to_utf16(*submit_error));
			return;
		}
		set_transient_status(L"已开始发送拖放内容");
	}

	void handle_close_request() {
		if (close_requested_) {
			return;
		}

		const auto send_snapshot = send_server_.snapshot();
		const auto receive_snapshot = receive_progress_->snapshot();
		const bool active_transfer = send_snapshot.transfer_in_progress
			|| (is_receive_session_active() && receive_snapshot.status_text == "receiving");
		if (active_transfer) {
			auto* dlg = new wxMessageDialog(this,
				L"传输或连接仍在进行，要停止吗？",
				L"确认关闭",
				wxYES_NO | wxNO_DEFAULT | wxICON_QUESTION);
			dlg->SetYesNoLabels(L"是", L"否");
			if (dlg->ShowModal() != wxID_YES) {
				dlg->Destroy();
				return;
			}
			dlg->Destroy();
		}

		close_requested_ = true;
		if (receive_cancel_event_) {
			receive_cancel_event_->emit();
		}
		if (receive_thread_.joinable()) {
			receive_thread_.request_stop();
		}
		Hide();
	}

	std::optional<std::string> selected_send_address_url() const {
		if (send_addr_choice_ == nullptr) {
			return std::nullopt;
		}
		auto selected_index = send_addr_choice_->GetSelection();
		if (selected_index == wxNOT_FOUND || static_cast<std::size_t>(selected_index) >= addresses_.size()) {
			return std::nullopt;
		}
		return addresses_[static_cast<std::size_t>(selected_index)].url;
	}

	void rebuild_address_choice(std::string_view preferred_url = {}) {
		send_addr_choice_->Clear();

		int selected_index = -1;
		for (std::size_t index = 0; index < addresses_.size(); ++index) {
			send_addr_choice_->AppendString(utf8_to_utf16(addresses_[index].url));
			if (!preferred_url.empty() && addresses_[index].url == preferred_url) {
				selected_index = static_cast<int>(index);
			}
		}
		if (selected_index < 0 && !addresses_.empty()) {
			selected_index = 0;
		}
		if (selected_index >= 0) {
			send_addr_choice_->SetSelection(selected_index);
		}
	}

	void update_send_addresses(std::uint16_t port) {
		if (port == 0 || address_port_ == port) {
			return;
		}
		address_port_ = port;
		try {
			const auto selected_url = selected_send_address_url();
			addresses_ = enumerate_shareable_addresses(port);
			rebuild_address_choice(selected_url.value_or(""));
		} catch (const std::exception& error) {
			send_progress_->set_failed(error.what());
		}
	}

	void update_send_controls(const GuiSendServerSnapshot& snapshot) {
		if (snapshot.receiver_connected) {
			send_addr_title_label_->Hide();
			send_addr_choice_->Hide();
			copy_button_->Hide();
			send_empty_label_->Hide();
			drop_target_->Show();
			if (snapshot.transfer_in_progress) {
				drop_target_->set_accepting(false);
				drop_target_->set_message(L"正在发送当前拖放内容，请等待本次传输完成。");
			} else {
				drop_target_->set_accepting(true);
				drop_target_->set_message(L"接收端已连接。\n将文件或文件夹拖到这里开始发送。");
			}
			return;
		}

		drop_target_->Hide();
		send_addr_title_label_->Show();
		if (snapshot.bound_port == 0) {
			send_addr_choice_->Hide();
			copy_button_->Hide();
			send_empty_label_->Show();
			send_empty_label_->SetLabelText(L"正在准备发送地址...");
			return;
		}
		if (addresses_.empty()) {
			send_addr_choice_->Hide();
			copy_button_->Hide();
			send_empty_label_->Show();
			send_empty_label_->SetLabelText(
				L"暂未找到可分享的非虚拟网卡地址。\n\n如果你刚切换网络，请稍后再试。\n接收端连入后，这里会自动切换为拖放框。");
			return;
		}
		send_empty_label_->Hide();
		send_addr_choice_->Show();
		copy_button_->Show();
	}

	void update_receive_controls() {
		// current_dir_ 不会在运行时改变，目录标签在 build_receive_tab 中已设置
		const bool active = is_receive_session_active();
		receive_input_->Enable(!active);
		connect_button_->Enable(!active);
	}

	void update_progress_view_state(const TransferProgressSnapshot& snapshot, ProgressViewState& state,
	                                  std::chrono::steady_clock::time_point now) {
		const bool counters_reset = snapshot.processed_bytes < state.last_bytes
			|| snapshot.processed_files < state.last_files;
		const bool phase_reset = snapshot.status_text != state.last_status_text
			&& snapshot.processed_bytes == 0 && snapshot.processed_files == 0;
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
			// current_dir_ 在运行时不变，使用构造时缓存的文本
			detail_label_->SetLabelText(receive_detail_text_);
			return;
		}

		if (send_snapshot.bound_port == 0) {
			detail_label_->SetLabelText(L"发送页：正在绑定监听端口...");
			return;
		}
		if (send_snapshot.receiver_connected) {
			detail_label_->SetLabelText(
				L"发送页：端口 " + std::to_wstring(send_snapshot.bound_port) + L"    接收端已连入，可等待拖放开始发送");
			return;
		}
		detail_label_->SetLabelText(
			L"发送页：端口 " + std::to_wstring(send_snapshot.bound_port) + L"    复制当前所选链接给接收端");
	}

	void update_summary_boxes(const TransferProgressSnapshot& snapshot, const ProgressViewState& state,
	                           bool receive_selected, std::chrono::steady_clock::time_point now) {
		const auto average_rate_end_time = state.completed_at.value_or(now);
		const std::wstring io_label = receive_selected ? L"磁盘写入" : L"磁盘读取";
		recent_label_->SetLabelText(
			io_label + L"（最近 1 秒）：" + utf8_to_utf16(format_rate(state.recent_rate))
			+ L"    最近 1 秒文件数：" + std::to_wstring(state.recent_files));
		total_label_->SetLabelText(
			io_label + L"（累计）：" + utf8_to_utf16(format_size(snapshot.processed_bytes))
			+ L"    平均速率：" + utf8_to_utf16(format_average_rate(snapshot.processed_bytes, average_rate_end_time - state.started_at))
			+ L"    累计文件数：" + std::to_wstring(snapshot.processed_files));
	}

	void update_status_box(const TransferProgressSnapshot& snapshot) {
		std::wstring status_text = utf8_to_utf16(snapshot.status_text);
		if (!transient_status_.empty() && std::chrono::steady_clock::now() >= transient_status_until_) {
			transient_status_.clear();
		}
		if (!transient_status_.empty()) {
			if (!status_text.empty()) {
				status_text += L"    ";
			}
			status_text += transient_status_;
		}
		status_label_->SetLabelText(L"状态：" + status_text);
	}

	void update_ui() {
		cleanup_finished_receive_session();
		maybe_finish_close_request();

		const auto now = std::chrono::steady_clock::now();
		const auto send_snapshot = send_server_.snapshot();

		// 地址枚举只需要执行一次，不应在定时器中反复轮询
		if (!addresses_initialized_) {
			update_send_addresses(send_snapshot.bound_port);
			if (address_port_ != 0) {
				addresses_initialized_ = true;
			}
		}

		update_send_controls(send_snapshot);
		update_receive_controls();

		const auto send_progress_snapshot = send_progress_->snapshot();
		const auto receive_progress_snapshot = receive_progress_->snapshot();
		update_progress_view_state(send_progress_snapshot, send_view_state_, now);
		update_progress_view_state(receive_progress_snapshot, receive_view_state_, now);

		const bool receive_selected = notebook_->GetSelection() == 1;
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
	bool addresses_initialized_ = false;
	std::vector<InterfaceAddress> addresses_;
	ProgressViewState send_view_state_;
	ProgressViewState receive_view_state_;
	bool close_requested_ = false;
	std::wstring transient_status_;
	std::chrono::steady_clock::time_point transient_status_until_{};
	std::wstring receive_detail_text_; // 缓存的接收页 detail 文本（current_dir_ 不变）

	// Widgets
	wxStaticText* title_label_ = nullptr;
	wxNotebook* notebook_ = nullptr;
	wxPanel* send_panel_ = nullptr;
	wxPanel* receive_panel_ = nullptr;
	wxStaticText* send_intro_label_ = nullptr;
	wxStaticText* send_addr_title_label_ = nullptr;
	wxChoice* send_addr_choice_ = nullptr;
	wxButton* copy_button_ = nullptr;
	wxStaticText* send_empty_label_ = nullptr;
	DropTargetPanel* drop_target_ = nullptr;
	wxStaticText* receive_dest_label_ = nullptr;
	wxTextCtrl* receive_input_ = nullptr;
	wxButton* connect_button_ = nullptr;
	wxStaticText* receive_hint_label_ = nullptr;
	wxStaticText* detail_label_ = nullptr;
	wxStaticText* recent_label_ = nullptr;
	wxStaticText* total_label_ = nullptr;
	wxStaticText* status_label_ = nullptr;
	wxTimer* timer_ = nullptr;
};

// wxApp class
class SoraTransportApp : public wxApp {
public:
	bool OnInit() override {
		if (!wxApp::OnInit()) {
			return false;
		}

		const auto current_dir = std::filesystem::current_path();
		std::optional<SoratransUrl> clipboard_url;
		if (const auto clipboard = read_clipboard_text()) {
			clipboard_url = parse_soratrans_url(*clipboard);
		}

		auto* frame = new SoraTransportFrame(current_dir, clipboard_url);
		frame->Show(true);
		return true;
	}
};

wxIMPLEMENT_APP_NO_MAIN(SoraTransportApp);

int run_gui_app(int argc, char** argv) {
	return wxEntry(argc, argv);
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
