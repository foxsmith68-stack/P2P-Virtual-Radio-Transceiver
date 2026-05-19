#!/usr/bin/env python3
"""
Serverless P2P Virtual Radio Transceiver
Decentralized, serverless Peer-to-Peer virtual radio transceiver with real-time PTT voice.
"""

import sys
import json
import asyncio
import socket
import struct
import random
import time
import os
import base64
from collections import OrderedDict
from typing import Optional

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QTextEdit, QHeaderView, QComboBox, QFrame, QSplitter, QAbstractItemView,
    QMessageBox, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QPalette, QColor, QTextCursor, QKeyEvent
from qasync import QEventLoop, asyncSlot

# ---------------------------------------------------------------------------
# Audio availability check
# ---------------------------------------------------------------------------

try:
    import pyaudio
    HAS_AUDIO = True
except ImportError:
    HAS_AUDIO = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APP_PORT = 49710
APP_PORT_MAX_TRIES = 10

STUN_SERVER = "stun.l.google.com"
STUN_PORT = 19302
STUN_MAGIC = 0x2112A442
STUN_TIMEOUT = 3.0

HEARTBEAT_INTERVAL = 7.0
PEER_TIMEOUT = 30.0
REGISTRY_CLEAN_INTERVAL = 10.0
VIRTUAL_BW = 3000

FREQ_MIN = 100_000
FREQ_MAX = 30_000_000_000
DEFAULT_FREQ = 14_250_000

STEP_SIZES = [100, 1_000, 10_000, 100_000, 1_000_000, 100_000_000]
STEP_LABELS = ["100 Hz", "1 kHz", "10 kHz", "100 kHz", "1 MHz", "100 MHz"]

BROADCAST_ADDR = "255.255.255.255"

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "p2p_radio_peers.json")

# Audio configuration
AUDIO_FORMAT = 8  # pyaudio.paInt16 is 8
AUDIO_CHANNELS = 1
AUDIO_RATE = 16000
AUDIO_CHUNK = 1024

# ---------------------------------------------------------------------------
# Audio Manager (PyAudio wrapper)
# ---------------------------------------------------------------------------

class AudioManager:
    def __init__(self):
        self._audio = pyaudio.PyAudio()
        self._input_stream = None
        self._output_stream = None

    @property
    def available(self):
        return True

    def open_input_stream(self):
        if self._input_stream is not None:
            return
        self._input_stream = self._audio.open(
            format=AUDIO_FORMAT,
            channels=AUDIO_CHANNELS,
            rate=AUDIO_RATE,
            input=True,
            frames_per_buffer=AUDIO_CHUNK,
        )

    def close_input_stream(self):
        if self._input_stream is not None:
            try:
                self._input_stream.stop_stream()
                self._input_stream.close()
            except Exception:
                pass
            self._input_stream = None

    def open_output_stream(self):
        if self._output_stream is not None:
            return
        self._output_stream = self._audio.open(
            format=AUDIO_FORMAT,
            channels=AUDIO_CHANNELS,
            rate=AUDIO_RATE,
            output=True,
            frames_per_buffer=AUDIO_CHUNK,
        )

    def close_output_stream(self):
        if self._output_stream is not None:
            try:
                self._output_stream.stop_stream()
                self._output_stream.close()
            except Exception:
                pass
            self._output_stream = None

    def read_chunk(self):
        if self._input_stream is None:
            return b"\x00" * (AUDIO_CHUNK * 2)
        return self._input_stream.read(AUDIO_CHUNK, exception_on_overflow=False)

    def write_chunk(self, data: bytes):
        if self._output_stream is not None:
            self._output_stream.write(data)

    def close_all(self):
        self.close_input_stream()
        self.close_output_stream()
        try:
            self._audio.terminate()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# STUN helpers
# ---------------------------------------------------------------------------

def _make_stun_binding_request():
    msg_type = 0x0001
    tid = random.randbytes(12)
    header = struct.pack("!HHI", msg_type, 0, STUN_MAGIC)
    return header + tid, tid


def _parse_stun_response(data, sent_tid):
    if len(data) < 20:
        return None, None
    _, msg_len, magic = struct.unpack("!HHI", data[:8])
    tid = data[8:20]
    if magic != STUN_MAGIC or tid != sent_tid:
        return None, None
    pos = 20
    actual_len = min(msg_len + 20, len(data))
    while pos + 4 <= actual_len:
        attr_type, attr_len = struct.unpack("!HH", data[pos:pos+4])
        pos += 4
        if attr_type == 0x0020:
            end = pos + attr_len
            if end > len(data):
                break
            family = data[pos + 1]
            if family == 0x01 and attr_len >= 8:
                xor_port = struct.unpack("!H", data[pos+2:pos+4])[0]
                port = xor_port ^ (STUN_MAGIC >> 16)
                addr_bytes = data[pos+4:pos+8]
                magic_bytes = struct.pack("!I", STUN_MAGIC)
                ip_bytes = bytes(a ^ b for a, b in zip(addr_bytes, magic_bytes))
                ip = socket.inet_ntoa(ip_bytes)
                return ip, port
        pos += attr_len
        pad = (4 - (attr_len % 4)) % 4
        pos += pad
    return None, None


class STUNProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.future = asyncio.get_event_loop().create_future()
        self.transaction_id = None
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if self.transaction_id is not None and not self.future.done():
            ip, port = _parse_stun_response(data, self.transaction_id)
            self.future.set_result((ip, port))

    def error_received(self, exc):
        if not self.future.done():
            self.future.set_exception(exc)


async def stun_discovery():
    try:
        loop = asyncio.get_event_loop()
        stun_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        stun_sock.setblocking(False)
        stun_sock.bind(("0.0.0.0", 0))

        protocol = STUNProtocol()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol, sock=stun_sock
        )

        req, tid = _make_stun_binding_request()
        protocol.transaction_id = tid
        transport.sendto(req, (STUN_SERVER, STUN_PORT))

        try:
            result = await asyncio.wait_for(protocol.future, timeout=STUN_TIMEOUT)
            return result
        except asyncio.TimeoutError:
            return None, None
        finally:
            transport.close()
    except Exception:
        return None, None


def _get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("8.8.8.8", 53))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _bind_udp_socket():
    for port in range(APP_PORT, APP_PORT + APP_PORT_MAX_TRIES):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            sock.bind(("0.0.0.0", port))
            sock.setblocking(False)
            return sock, port
        except OSError:
            sock.close()
            continue
    raise RuntimeError(f"Could not bind UDP socket on any port in range {APP_PORT}-{APP_PORT + APP_PORT_MAX_TRIES - 1}")


def format_freq(freq: int) -> str:
    return f"{freq:,} Hz"


def freq_to_mhz(freq: int) -> str:
    return f"{freq / 1_000_000:.6f}"


# ---------------------------------------------------------------------------
# Network signal bridge (thread-safe Qt signalling from asyncio)
# ---------------------------------------------------------------------------

class NetworkBridge(QObject):
    heartbeat_received = pyqtSignal(str, int, float, str)
    transmit_received = pyqtSignal(str, int, str)
    audio_received = pyqtSignal(str, int, str)


# ---------------------------------------------------------------------------
# P2P Network Protocol (asyncio.DatagramProtocol)
# ---------------------------------------------------------------------------

class P2PNetworkProtocol(asyncio.DatagramProtocol):
    def __init__(self, bridge: NetworkBridge):
        self.bridge = bridge
        self.transport = None
        self.own_callsign = "ANONYMOUS"
        self.own_frequency = DEFAULT_FREQ

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            msg = json.loads(data.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        msg_type = msg.get("type")
        ip_port = f"{addr[0]}:{addr[1]}"

        if msg_type == "HEARTBEAT":
            callsign = msg.get("callsign", "?")
            freq = msg.get("frequency", 0)
            ts = msg.get("timestamp", 0.0)
            self.bridge.heartbeat_received.emit(callsign, freq, ts, ip_port)

        elif msg_type == "TRANSMIT":
            callsign = msg.get("callsign", "?")
            freq = msg.get("frequency", 0)
            payload = msg.get("payload", "")
            self.bridge.transmit_received.emit(callsign, freq, payload)

        elif msg_type == "AUDIO":
            callsign = msg.get("callsign", "?")
            freq = msg.get("frequency", 0)
            audio_data = msg.get("audio_data", "")
            self.bridge.audio_received.emit(callsign, freq, audio_data)

    def send_to(self, payload: str, addr) -> None:
        if self.transport is not None:
            try:
                self.transport.sendto(payload.encode("utf-8"), addr)
            except OSError:
                pass

    def broadcast(self, payload: str, port: int) -> None:
        if self.transport is not None:
            try:
                self.transport.sendto(payload.encode("utf-8"), (BROADCAST_ADDR, port))
            except OSError:
                pass

    def make_heartbeat(self) -> str:
        return json.dumps({
            "type": "HEARTBEAT",
            "callsign": self.own_callsign,
            "frequency": self.own_frequency,
            "timestamp": time.time()
        })

    def make_transmit(self, payload: str) -> str:
        return json.dumps({
            "type": "TRANSMIT",
            "callsign": self.own_callsign,
            "frequency": self.own_frequency,
            "payload": payload
        })


# ---------------------------------------------------------------------------
# Peer registry
# ---------------------------------------------------------------------------

PeerDict = OrderedDict

# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("P2P Virtual Radio Transceiver")
        self.setMinimumSize(900, 650)

        # State
        self.callsign = "ANONYMOUS"
        self.frequency = DEFAULT_FREQ
        self.step_index = 1
        self.peers: PeerDict = OrderedDict()
        self.protocol: Optional[P2PNetworkProtocol] = None
        self.transport = None
        self.bound_port = APP_PORT
        self.external_ip = None
        self.external_port = None
        self.local_ip = "127.0.0.1"

        # PTT / Audio state
        self.ptt_active = False
        self.ptt_task: Optional[asyncio.Task] = None
        self._capture_task: Optional[asyncio.Task] = None
        self.audio_manager: Optional[AudioManager] = None
        self._audio_tx_queue: Optional[asyncio.Queue] = None
        self._audio_write_lock = asyncio.Lock()

        # Bridge network signals to Qt main thread
        self.bridge = NetworkBridge()
        self.bridge.heartbeat_received.connect(self._on_heartbeat)
        self.bridge.transmit_received.connect(self._on_transmit)
        self.bridge.audio_received.connect(self._on_audio)

        self._build_ui()
        self._apply_dark_theme()
        self._set_rx_visual_state()
        self._update_vfo_display()

        # Warn if audio unavailable
        if not HAS_AUDIO:
            self._log_rx("[SYS] PyAudio not installed — voice TX/RX disabled. "
                         "Install with: pip install pyaudio")

    # ------------------------------------------------------------------
    # UI Construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(6)

        # ---- Top Control Bar ----
        top_bar = QHBoxLayout()

        callsign_layout = QVBoxLayout()
        callsign_layout.setSpacing(1)
        lbl_callsign = QLabel("Callsign")
        lbl_callsign.setStyleSheet("font-size: 10px; color: #aaa;")
        self.callsign_input = QLineEdit()
        self.callsign_input.setPlaceholderText("ANONYMOUS")
        self.callsign_input.setMaxLength(12)
        self.callsign_input.setFixedWidth(140)
        self.callsign_input.returnPressed.connect(self._on_callsign_changed)
        callsign_layout.addWidget(lbl_callsign)
        callsign_layout.addWidget(self.callsign_input)

        self.vfo_display = QLabel()
        self.vfo_display.setAlignment(Qt.AlignmentFlag.AlignCenter)
        vfo_font = QFont("Consolas", 28, QFont.Weight.Bold)
        self.vfo_display.setFont(vfo_font)
        self.vfo_display.setFixedHeight(60)
        self.vfo_display.setMinimumWidth(380)

        top_bar.addLayout(callsign_layout)
        top_bar.addSpacing(12)
        top_bar.addWidget(self.vfo_display, 1)

        main_layout.addLayout(top_bar)

        # ---- Tuning Controls ----
        tune_layout = QHBoxLayout()
        tune_layout.setSpacing(6)

        self.btn_tune_down = QPushButton("-")
        self.btn_tune_down.setFixedWidth(46)
        self.btn_tune_down.setFixedHeight(36)
        self.btn_tune_down.clicked.connect(self._on_tune_down)

        self.freq_entry = QLineEdit()
        self.freq_entry.setPlaceholderText("Enter freq in Hz")
        self.freq_entry.setFixedWidth(200)
        self.freq_entry.returnPressed.connect(self._on_freq_entry)

        self.btn_tune_up = QPushButton("+")
        self.btn_tune_up.setFixedWidth(46)
        self.btn_tune_up.setFixedHeight(36)
        self.btn_tune_up.clicked.connect(self._on_tune_up)

        self.step_selector = QComboBox()
        self.step_selector.addItems(STEP_LABELS)
        self.step_selector.setCurrentIndex(self.step_index)
        self.step_selector.currentIndexChanged.connect(self._on_step_changed)

        tune_layout.addWidget(self.btn_tune_down)
        tune_layout.addWidget(self.freq_entry)
        tune_layout.addWidget(self.btn_tune_up)
        tune_layout.addSpacing(8)
        tune_layout.addWidget(QLabel("Step:"))
        tune_layout.addWidget(self.step_selector)
        tune_layout.addStretch(1)

        main_layout.addLayout(tune_layout)

        # ---- PTT Button ----
        ptt_layout = QHBoxLayout()
        ptt_layout.setSpacing(8)

        self.ptt_button = QPushButton("  PTT (Transmit Voice)  ")
        self.ptt_button.setFixedHeight(44)
        ptt_btn_font = QFont("Consolas", 12, QFont.Weight.Bold)
        self.ptt_button.setFont(ptt_btn_font)
        self.ptt_button.setCheckable(True)
        self.ptt_button.pressed.connect(self._start_ptt)
        self.ptt_button.released.connect(self._stop_ptt)
        self.ptt_button.setStyleSheet(
            "QPushButton { background: #2a5a2a; color: #00ff88; border: 2px solid #00ff88; "
            "border-radius: 6px; padding: 8px 20px; }"
            "QPushButton:hover { background: #3a7a3a; }"
            "QPushButton:checked { background: #5a2a2a; color: #ff4444; border-color: #ff4444; }"
        )

        self.ptt_hint = QLabel("or hold Spacebar")
        self.ptt_hint.setStyleSheet("color: #888; font-size: 10px;")

        ptt_layout.addStretch(1)
        ptt_layout.addWidget(self.ptt_button)
        ptt_layout.addWidget(self.ptt_hint)
        ptt_layout.addStretch(1)

        main_layout.addLayout(ptt_layout)

        # ---- Station Directory + Rx/Tx split ----
        splitter = QSplitter(Qt.Orientation.Horizontal)

        station_container = QWidget()
        station_layout = QVBoxLayout(station_container)
        station_layout.setContentsMargins(0, 0, 0, 0)
        station_layout.setSpacing(2)

        lbl_stations = QLabel("Station Directory")
        lbl_stations.setStyleSheet("font-weight: bold; color: #ccc; font-size: 11px;")

        self.station_table = QTableWidget(0, 3)
        self.station_table.setHorizontalHeaderLabels(["Callsign", "Frequency (MHz)", "Last Seen"])
        self.station_table.horizontalHeader().setStretchLastSection(True)
        self.station_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.station_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.station_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.station_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.station_table.verticalHeader().setVisible(False)
        self.station_table.doubleClicked.connect(self._on_station_double_clicked)
        self.station_table.setAlternatingRowColors(True)

        station_layout.addWidget(lbl_stations)
        station_layout.addWidget(self.station_table)

        splitter.addWidget(station_container)

        comm_container = QWidget()
        comm_layout = QVBoxLayout(comm_container)
        comm_layout.setContentsMargins(0, 0, 0, 0)
        comm_layout.setSpacing(2)

        lbl_rx = QLabel("Receive (Rx)")
        lbl_rx.setStyleSheet("font-weight: bold; color: #ccc; font-size: 11px;")

        self.rx_display = QTextEdit()
        self.rx_display.setReadOnly(True)
        rx_font = QFont("Consolas", 10)
        self.rx_display.setFont(rx_font)
        self.rx_display.setStyleSheet("background: #0a0a0a; color: #cccccc; border: 1px solid #333;")

        lbl_tx = QLabel("Transmit (Tx)")
        lbl_tx.setStyleSheet("font-weight: bold; color: #ccc; font-size: 11px;")

        self.tx_input = QLineEdit()
        self.tx_input.setPlaceholderText("Type message and press Enter to transmit...")
        self.tx_input.returnPressed.connect(self._on_transmit)

        comm_layout.addWidget(lbl_rx)
        comm_layout.addWidget(self.rx_display, 1)
        comm_layout.addWidget(lbl_tx)
        comm_layout.addWidget(self.tx_input)

        splitter.addWidget(comm_container)
        splitter.setSizes([320, 580])

        main_layout.addWidget(splitter, 1)

        self.status_label = QLabel("Starting...")
        self.status_label.setStyleSheet("color: #888; font-size: 10px; padding: 2px 0;")
        main_layout.addWidget(self.status_label)

    def _apply_dark_theme(self):
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Window, QColor(30, 30, 30))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(220, 220, 220))
        palette.setColor(QPalette.ColorRole.Base, QColor(20, 20, 20))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(35, 35, 35))
        palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(40, 40, 40))
        palette.setColor(QPalette.ColorRole.ToolTipText, QColor(220, 220, 220))
        palette.setColor(QPalette.ColorRole.Text, QColor(220, 220, 220))
        palette.setColor(QPalette.ColorRole.Button, QColor(45, 45, 45))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(220, 220, 220))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 100, 100))
        palette.setColor(QPalette.ColorRole.Link, QColor(80, 160, 255))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(60, 120, 220))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
        self.setPalette(palette)

        self.setStyleSheet("""
            QToolTip { background: #333; border: 1px solid #555; padding: 2px; }
            QHeaderView::section { background: #2a2a2a; color: #ccc; border: 1px solid #444; padding: 4px; }
            QTableWidget { gridline-color: #3a3a3a; }
            QLineEdit { background: #1a1a1a; color: #ddd; border: 1px solid #555;
                         padding: 4px 6px; border-radius: 3px; }
            QLineEdit:focus { border-color: #0088ff; }
            QPushButton { background: #3a3a3a; color: #ddd; border: 1px solid #555;
                          border-radius: 4px; padding: 4px 14px; }
            QPushButton:hover { background: #4a4a4a; border-color: #888; }
            QPushButton:pressed { background: #555; }
            QComboBox { background: #1a1a1a; color: #ddd; border: 1px solid #555;
                        padding: 4px 6px; border-radius: 3px; }
            QComboBox:focus { border-color: #0088ff; }
            QComboBox QAbstractItemView { background: #2a2a2a; color: #ddd; selection-background-color: #3a6aaf; }
            QSplitter::handle { background: #444; width: 3px; }
        """)

    # ------------------------------------------------------------------
    # VFO visual state (RX green / TX red)
    # ------------------------------------------------------------------

    def _set_rx_visual_state(self):
        self.vfo_display.setStyleSheet(
            "color: #00ff88; background: #111; padding: 6px 18px; "
            "border: 2px solid #00ff88; border-radius: 6px;"
        )

    def _set_tx_visual_state(self):
        self.vfo_display.setStyleSheet(
            "color: #ff4444; background: #1a0000; padding: 6px 18px; "
            "border: 2px solid #ff4444; border-radius: 6px;"
        )

    def _update_vfo_display(self):
        if self.ptt_active:
            self.vfo_display.setText("[TX]  " + format_freq(self.frequency))
        else:
            self.vfo_display.setText("[RX]  " + format_freq(self.frequency))

    # ------------------------------------------------------------------
    # VFO / Frequency helpers
    # ------------------------------------------------------------------

    def _clamp_frequency(self, freq: int) -> int:
        return max(FREQ_MIN, min(FREQ_MAX, freq))

    def _set_frequency(self, freq: int):
        self.frequency = self._clamp_frequency(freq)
        self._update_vfo_display()
        if self.protocol is not None:
            self.protocol.own_frequency = self.frequency

    # ------------------------------------------------------------------
    # UI event handlers
    # ------------------------------------------------------------------

    def _on_callsign_changed(self):
        text = self.callsign_input.text().strip().upper()
        if not text:
            text = "ANONYMOUS"
        self.callsign = text
        if self.protocol is not None:
            self.protocol.own_callsign = self.callsign

    def _on_tune_down(self):
        step = STEP_SIZES[self.step_index]
        self._set_frequency(self.frequency - step)

    def _on_tune_up(self):
        step = STEP_SIZES[self.step_index]
        self._set_frequency(self.frequency + step)

    def _on_freq_entry(self):
        text = self.freq_entry.text().strip().replace(",", "")
        try:
            freq = int(text)
            self._set_frequency(freq)
        except ValueError:
            try:
                freq = int(float(text))
                self._set_frequency(freq)
            except ValueError:
                pass
        self.freq_entry.clear()

    def _on_step_changed(self, index):
        self.step_index = index

    def _on_station_double_clicked(self, index):
        row = index.row()
        callsign_item = self.station_table.item(row, 0)
        freq_item = self.station_table.item(row, 1)
        if callsign_item and freq_item:
            try:
                freq_hz = int(float(freq_item.text().replace(",", "")) * 1_000_000)
                self._set_frequency(freq_hz)
                self._log_rx(f"[INFO] Tuned to {callsign_item.text()} on {format_freq(freq_hz)}")
            except ValueError:
                pass

    def _on_transmit(self):
        message = self.tx_input.text().strip()
        if not message:
            return
        self.tx_input.clear()

        self._send_text_packet(message)
        self._log_rx(f"[{self.callsign}] {message}")

    def _send_text_packet(self, message: str):
        if self.protocol is None or self.protocol.transport is None:
            return
        packet = self.protocol.make_transmit(message)
        addrs = []
        for ip_port in list(self.peers.keys()):
            parts = ip_port.split(":")
            if len(parts) == 2:
                try:
                    addrs.append((parts[0], int(parts[1])))
                except (ValueError, OSError):
                    pass
        try:
            self.protocol.broadcast(packet, self.bound_port)
        except Exception:
            pass
        for addr in addrs:
            try:
                self.protocol.send_to(packet, addr)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # PTT / Voice transmission
    # ------------------------------------------------------------------

    def _start_ptt(self):
        if self.ptt_active:
            return

        if not HAS_AUDIO:
            self._log_rx("[WARN] PyAudio not available — cannot transmit voice")
            self.ptt_button.setChecked(False)
            return

        if self.audio_manager is None:
            try:
                self.audio_manager = AudioManager()
                self.audio_manager.open_output_stream()
            except Exception as e:
                self._log_rx(f"[WARN] Audio init failed: {e}")
                self.ptt_button.setChecked(False)
                return

        try:
            self.audio_manager.open_input_stream()
        except Exception as e:
            self._log_rx(f"[WARN] Cannot open microphone: {e}")
            self.ptt_button.setChecked(False)
            return

        self.ptt_active = True
        self._audio_tx_queue = asyncio.Queue(maxsize=20)
        self._set_tx_visual_state()
        self._update_vfo_display()
        self.ptt_button.setChecked(True)
        self._capture_task = asyncio.ensure_future(self._audio_capture_loop())
        self.ptt_task = asyncio.ensure_future(self._tx_audio_loop())
        self._log_rx(f"[TX] Voice transmission active")

    def _stop_ptt(self):
        if not self.ptt_active:
            return

        self.ptt_active = False

        if self._capture_task is not None:
            self._capture_task.cancel()
            self._capture_task = None
        if self.ptt_task is not None:
            self.ptt_task.cancel()
            self.ptt_task = None

        self.audio_manager.close_input_stream()
        self.ptt_button.setChecked(False)

        self._set_rx_visual_state()
        self._update_vfo_display()
        self._log_rx(f"[TX] Voice transmission ended")

    async def _audio_capture_loop(self):
        """Read raw PCM chunks from mic into a bounded queue (drop oldest if full)."""
        loop = asyncio.get_event_loop()
        try:
            while self.ptt_active:
                raw = await loop.run_in_executor(None, self.audio_manager.read_chunk)
                if self._audio_tx_queue.full():
                    try:
                        self._audio_tx_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                await self._audio_tx_queue.put(raw)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log_rx(f"[WARN] Audio capture error: {e}")

    async def _tx_audio_loop(self):
        """Consume queued audio chunks, encode, and transmit to all peers."""
        try:
            while self.ptt_active:
                try:
                    raw = self._audio_tx_queue.get_nowait()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.005)
                    continue
                encoded = base64.b64encode(raw).decode("ascii")
                packet = json.dumps({
                    "type": "AUDIO",
                    "callsign": self.callsign,
                    "frequency": self.frequency,
                    "audio_data": encoded
                })
                for ip_port in list(self.peers.keys()):
                    parts = ip_port.split(":")
                    if len(parts) == 2:
                        try:
                            self.protocol.send_to(packet, (parts[0], int(parts[1])))
                        except Exception:
                            pass
                try:
                    self.protocol.broadcast(packet, self.bound_port)
                except Exception:
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log_rx(f"[WARN] TX network error: {e}")

    # ------------------------------------------------------------------
    # Signal handlers (network -> UI thread)
    # ------------------------------------------------------------------

    def _on_heartbeat(self, callsign: str, frequency: int, timestamp: float, ip_port: str):
        self.peers[ip_port] = {
            "callsign": callsign,
            "frequency": frequency,
            "timestamp": timestamp
        }
        self._refresh_station_table()

    def _on_transmit(self, callsign: str, frequency: int, payload: str):
        if abs(frequency - self.frequency) <= VIRTUAL_BW:
            self._log_rx(f"[{callsign}] {payload}")

    def _on_audio(self, callsign: str, frequency: int, audio_b64: str):
        if not HAS_AUDIO:
            return
        if abs(frequency - self.frequency) > VIRTUAL_BW:
            return
        if self.audio_manager is None:
            try:
                self.audio_manager = AudioManager()
                self.audio_manager.open_output_stream()
            except Exception:
                return

        try:
            decoded = base64.b64decode(audio_b64)
        except Exception:
            return

        asyncio.ensure_future(self._play_audio(decoded))

    async def _play_audio(self, decoded: bytes):
        if self.audio_manager is None:
            return
        async with self._audio_write_lock:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.audio_manager.write_chunk, decoded)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Key events (Spacebar PTT)
    # ------------------------------------------------------------------

    def keyPressEvent(self, event: QKeyEvent):
        if (event.key() == Qt.Key.Key_Space
                and not self.ptt_active
                and not self.tx_input.hasFocus()
                and not self.freq_entry.hasFocus()
                and not self.callsign_input.hasFocus()):
            self._start_ptt()
            event.accept()
        else:
            super().keyPressEvent(event)

    def keyReleaseEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key.Key_Space and self.ptt_active:
            self._stop_ptt()
            event.accept()
        else:
            super().keyReleaseEvent(event)

    # ------------------------------------------------------------------
    # Rx log
    # ------------------------------------------------------------------

    def _log_rx(self, text: str):
        cursor = self.rx_display.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        ts = time.strftime("%H:%M:%S")
        cursor.insertText(f"[{ts}] {text}\n")
        self.rx_display.setTextCursor(cursor)
        scrollbar = self.rx_display.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    # ------------------------------------------------------------------
    # Station table
    # ------------------------------------------------------------------

    def _refresh_station_table(self):
        self.station_table.setRowCount(0)
        now = time.time()
        for ip_port, info in list(self.peers.items()):
            row = self.station_table.rowCount()
            self.station_table.insertRow(row)

            cs_item = QTableWidgetItem(info["callsign"])
            cs_item.setData(Qt.ItemDataRole.UserRole, ip_port)

            mhz = freq_to_mhz(info["frequency"])
            freq_item = QTableWidgetItem(mhz)

            age = now - info["timestamp"]
            if age < 1:
                last_seen = "now"
            elif age < 60:
                last_seen = f"{int(age)}s ago"
            elif age < 3600:
                last_seen = f"{int(age // 60)}m ago"
            else:
                last_seen = f"{int(age // 3600)}h ago"
            time_item = QTableWidgetItem(last_seen)

            self.station_table.setItem(row, 0, cs_item)
            self.station_table.setItem(row, 1, freq_item)
            self.station_table.setItem(row, 2, time_item)

    # ------------------------------------------------------------------
    # Peer cleanup
    # ------------------------------------------------------------------

    def _clean_stale_peers(self):
        now = time.time()
        stale = [k for k, v in self.peers.items() if now - v["timestamp"] > PEER_TIMEOUT]
        for k in stale:
            del self.peers[k]
        if stale:
            self._refresh_station_table()

    # ------------------------------------------------------------------
    # Async lifecycle
    # ------------------------------------------------------------------

    async def start_async(self):
        self.local_ip = await asyncio.get_event_loop().run_in_executor(None, _get_local_ip)

        stun_result = await stun_discovery()
        if stun_result[0]:
            self.external_ip, self.external_port = stun_result
        else:
            self.external_ip = self.local_ip
            self.external_port = self.bound_port

        try:
            sock, self.bound_port = await asyncio.get_event_loop().run_in_executor(None, _bind_udp_socket)
        except RuntimeError as e:
            QMessageBox.critical(self, "Socket Error", str(e))
            QApplication.quit()
            return

        loop = asyncio.get_event_loop()
        self.protocol = P2PNetworkProtocol(self.bridge)
        self.protocol.own_callsign = self.callsign
        self.protocol.own_frequency = self.frequency
        _, _ = await loop.create_datagram_endpoint(lambda: self.protocol, sock=sock)

        self._log_rx(f"[SYS] Bound to port {self.bound_port}")
        self._log_rx(f"[SYS] Local IP: {self.local_ip}")
        self._log_rx(f"[SYS] External: {self.external_ip}:{self.external_port}")
        self._log_rx(f"[SYS] Monitoring {format_freq(FREQ_MIN)} to {format_freq(FREQ_MAX)}")
        self._log_rx(f"[SYS] Virtual bandwidth: {VIRTUAL_BW} Hz")
        self._log_rx(f"[SYS] Audio: {AUDIO_RATE} Hz / 16-bit / mono")
        if HAS_AUDIO:
            self._log_rx(f"[SYS] P2P Radio ready — {self.callsign} QRV (voice + text)")
        else:
            self._log_rx(f"[SYS] P2P Radio ready — {self.callsign} QRV (text only)")

        self.status_label.setText(
            f"Port: {self.bound_port}  |  Local: {self.local_ip}  |  "
            f"External: {self.external_ip}:{self.external_port}  |  "
            f"Peers: {len(self.peers)}"
        )

        await self._load_peers()

        asyncio.ensure_future(self._heartbeat_loop())
        asyncio.ensure_future(self._registry_cleanup_loop())
        asyncio.ensure_future(self._status_update_loop())

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL + random.uniform(-1, 1))
            if self.protocol is None or self.protocol.transport is None:
                continue
            packet = self.protocol.make_heartbeat()
            for ip_port in list(self.peers.keys()):
                parts = ip_port.split(":")
                if len(parts) == 2:
                    try:
                        self.protocol.send_to(packet, (parts[0], int(parts[1])))
                    except (ValueError, OSError):
                        pass
            try:
                self.protocol.broadcast(packet, self.bound_port)
            except Exception:
                pass

    async def _registry_cleanup_loop(self):
        while True:
            await asyncio.sleep(REGISTRY_CLEAN_INTERVAL)
            self._clean_stale_peers()

    async def _status_update_loop(self):
        while True:
            await asyncio.sleep(5)
            self.status_label.setText(
                f"Port: {self.bound_port}  |  Local: {self.local_ip}  |  "
                f"External: {self.external_ip}:{self.external_port}  |  "
                f"Peers: {len(self.peers)}"
            )

    async def _load_peers(self):
        def _read():
            if not os.path.exists(CONFIG_FILE):
                return []
            try:
                with open(CONFIG_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                return []

        peer_addrs = await asyncio.get_event_loop().run_in_executor(None, _read)
        for entry in peer_addrs:
            ip = entry.get("ip", "")
            port = entry.get("port", APP_PORT)
            ip_port = f"{ip}:{port}"
            if ip_port not in self.peers:
                self.peers[ip_port] = {
                    "callsign": "?",
                    "frequency": 0,
                    "timestamp": 0.0
                }
        if peer_addrs:
            self._log_rx(f"[SYS] Loaded {len(peer_addrs)} peer(s) from config")
            self._refresh_station_table()

    def save_peers(self):
        addrs = []
        for ip_port in list(self.peers.keys()):
            parts = ip_port.split(":")
            if len(parts) == 2:
                addrs.append({"ip": parts[0], "port": int(parts[1])})
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(addrs, f)
        except Exception:
            pass

    def on_quit(self):
        if self.ptt_active:
            self._stop_ptt()
        if self.audio_manager is not None:
            self.audio_manager.close_all()
        self.save_peers()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = MainWindow()
    window.show()

    asyncio.ensure_future(window.start_async())

    app.aboutToQuit.connect(window.on_quit)

    with loop:
        loop.run_forever()


if __name__ == "__main__":
    main()
