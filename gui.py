"""
Telco Customer Data Generator GUI

A desktop GUI application for generating realistic Dutch telecommunications
customer data with configurable parameters.
"""

import sys
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QLabel, QPushButton, QFrame, QTextEdit,
    QGroupBox, QScrollArea, QSizePolicy, QProgressBar,
    QSpinBox, QDoubleSpinBox, QComboBox, QTabWidget, QSlider,
    QSplitter, QFileDialog, QMessageBox, QLineEdit, QCheckBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QTextCursor, QIcon

from config import GeneratorConfig, SMALL_CONFIG, MEDIUM_CONFIG, LARGE_CONFIG
from generate import TelcoDataGenerator
from fabric_integration import FabricClient


# Application version
__version__ = "2.0.0"


# Color scheme (modern dark theme - VS Code inspired)
COLORS = {
    "background": "#1e1e2e",
    "surface": "#252536",
    "surface_light": "#2d2d42",
    "surface_hover": "#363652",
    "border": "#3d3d5c",
    "border_light": "#4d4d6d",
    "text": "#e0e0f0",
    "text_muted": "#9090b0",
    "text_dim": "#6060808",
    "accent": "#6c9eff",
    "accent_hover": "#8cb4ff",
    "accent_dim": "#4c7edf",
    "success": "#4ade80",
    "success_hover": "#6aee9a",
    "success_dim": "#2ab860",
    "warning": "#fbbf24",
    "warning_hover": "#fcd34d",
    "error": "#f87171",
    "error_hover": "#fca5a5",
    "info": "#60a5fa",
    "info_hover": "#93c5fd",
    "orange": "#fb923c",
    "purple": "#a78bfa",
    "cyan": "#22d3ee",
    "pink": "#f472b6",
    "gradient_start": "#6366f1",
    "gradient_end": "#8b5cf6",
}


class GeneratorWorker(QThread):
    """Background worker for data generation."""
    
    progress_updated = pyqtSignal(str, int)  # message, percentage
    generation_complete = pyqtSignal(dict)  # summary dict
    generation_error = pyqtSignal(str)  # error message
    
    def __init__(self, config: GeneratorConfig):
        super().__init__()
        self.config = config
        self.running = False
    
    def run(self):
        """Run the data generation."""
        self.running = True
        
        try:
            # Create generator
            self.progress_updated.emit("Initializing generator...", 5)
            generator = TelcoDataGenerator(self.config)
            
            # Generate data in steps
            self.progress_updated.emit("Generating products...", 10)
            generator.generate_products()
            
            self.progress_updated.emit("Generating parties and addresses...", 20)
            generator.generate_parties()
            
            self.progress_updated.emit("Generating accounts...", 35)
            generator.generate_accounts()
            
            self.progress_updated.emit("Generating subscribers...", 50)
            generator.generate_subscribers()
            
            self.progress_updated.emit("Generating subscriptions...", 65)
            generator.generate_subscriptions()
            
            self.progress_updated.emit("Generating services and orders...", 75)
            generator.generate_services()
            
            self.progress_updated.emit("Generating billing records...", 85)
            generator.generate_billing()
            
            self.progress_updated.emit("Generating support tickets...", 92)
            generator.generate_support_tickets()
            
            self.progress_updated.emit("Saving data...", 95)
            generator.save()
            
            self.progress_updated.emit("Complete!", 100)
            
            # Prepare summary
            summary = {
                "tables": {table: len(records) for table, records in generator.data.items()},
                "total_records": sum(len(records) for records in generator.data.values()),
                "output_dir": self.config.output_dir,
                "format": self.config.output_format
            }
            
            self.generation_complete.emit(summary)
            
        except Exception as e:
            self.generation_error.emit(str(e))
    
    def stop(self):
        """Stop the worker."""
        self.running = False


class FabricUploadWorker(QThread):
    """Background worker for Fabric workspace/lakehouse creation, and data upload."""
    
    progress_updated = pyqtSignal(str, int)  # message, percentage
    upload_complete = pyqtSignal(dict)  # summary
    upload_error = pyqtSignal(str)  # error message
    
    def __init__(self, workspace_name: str, lakehouse_name: str,
                 local_dir: str, file_extension: str, credential=None,
                 capacity_id: str = ""):
        super().__init__()
        self.workspace_name = workspace_name
        self.lakehouse_name = lakehouse_name
        self.local_dir = local_dir
        self.file_extension = file_extension
        self.credential = credential
        self.capacity_id = capacity_id
        self.log_messages = []
    
    def _log(self, msg: str):
        self.log_messages.append(msg)
        self.progress_updated.emit(msg, -1)  # -1 means don't update bar
    
    def run(self):
        try:
            client = FabricClient(log_callback=self._log, credential=self.credential)
            
            # Verify we have valid credentials (no re-authentication needed)
            if not client.is_authenticated():
                self.upload_error.emit("Not authenticated. Test connection first.")
                return
            
            self.progress_updated.emit("Using existing Fabric session...", 5)
            
            # Step 1: Get or create workspace (with capacity)
            self.progress_updated.emit(f"Setting up workspace '{self.workspace_name}'...", 10)
            ws = client.get_or_create_workspace(
                self.workspace_name,
                capacity_id=self.capacity_id or None,
            )
            workspace_id = ws["id"]
            
            # Step 3: Get or create lakehouse (without schema - Load Table API requirement)
            self.progress_updated.emit(f"Setting up lakehouse '{self.lakehouse_name}'...", 20)
            lh = client.get_or_create_lakehouse(
                workspace_id, self.lakehouse_name, enable_schemas=True
            )
            lakehouse_id = lh["id"]
            
            # Step 4: Upload files to OneLake (no table creation)
            self.progress_updated.emit("Uploading files to OneLake...", 25)
            
            def on_progress(filename, current, total):
                pct = 25 + int(70 * current / total)
                self.progress_updated.emit(f"Uploading: {filename} ({current}/{total})", pct)
            
            result = client.upload_and_load_tables(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                local_dir=self.local_dir,
                remote_folder="Files/telco_data",
                file_extension=self.file_extension,
                load_tables=False,  # Don't create tables
                progress_callback=on_progress,
            )
            
            self.progress_updated.emit("Complete!", 100)
            self.upload_complete.emit({
                "workspace": self.workspace_name,
                "workspace_id": workspace_id,
                "lakehouse": self.lakehouse_name,
                "lakehouse_id": lakehouse_id,
                "files_uploaded": result["files_uploaded"],
                "failed_files": result.get("failed_files", []),
            })
        
        except Exception as e:
            self.upload_error.emit(str(e))


class StyledButton(QPushButton):
    """Custom styled button."""
    
    def __init__(self, text: str, color: str = "accent", parent=None):
        super().__init__(text, parent)
        self.color = color
        self._apply_style()
    
    def _apply_style(self):
        color_map = {
            "accent": (COLORS["accent"], COLORS["accent_hover"], "#fff"),
            "success": (COLORS["success"], COLORS["success_hover"], "#000"),
            "warning": (COLORS["warning"], "#e5a82e", "#000"),
            "error": (COLORS["error"], COLORS["error_hover"], "#fff"),
            "default": (COLORS["surface_light"], COLORS["border"], COLORS["text"]),
            "info": (COLORS["info"], "#79b8ff", "#000"),
            "orange": (COLORS["orange"], "#f5a54f", "#000"),
            "purple": (COLORS["purple"], "#b687f9", "#000"),
        }
        
        bg, hover, fg = color_map.get(self.color, color_map["default"])
        
        self.setStyleSheet(f"""
            QPushButton {{
                background-color: {bg};
                color: {fg};
                border: none;
                border-radius: 6px;
                padding: 10px 20px;
                font-size: 14px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: {hover};
            }}
            QPushButton:pressed {{
                background-color: {bg};
            }}
            QPushButton:disabled {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text_muted"]};
            }}
        """)


class StyledGroupBox(QGroupBox):
    """Custom styled group box with modern card appearance."""
    
    def __init__(self, title: str, icon: str = "", parent=None):
        display_title = f"{icon}  {title}" if icon else title
        super().__init__(display_title, parent)
        self.setStyleSheet(f"""
            QGroupBox {{
                background-color: {COLORS["surface"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
                margin-top: 24px;
                padding: 16px;
                padding-top: 28px;
                font-size: 13px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                subcontrol-position: top left;
                top: 8px;
                left: 14px;
                padding: 4px 12px;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["gradient_start"]}, stop:1 {COLORS["gradient_end"]});
                border-radius: 6px;
                color: white;
                font-size: 12px;
                font-weight: 600;
            }}
        """)


class StyledSpinBox(QSpinBox):
    """Custom styled spin box."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setButtonSymbols(QSpinBox.ButtonSymbols.PlusMinus)
        self.setStyleSheet(f"""
            QSpinBox {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 13px;
                min-width: 80px;
            }}
            QSpinBox:focus {{
                border-color: {COLORS["accent"]};
            }}
            QSpinBox::up-button {{
                background-color: {COLORS["accent"]};
                border: none;
                border-radius: 2px;
                width: 18px;
                margin: 2px;
            }}
            QSpinBox::down-button {{
                background-color: {COLORS["error"]};
                border: none;
                border-radius: 2px;
                width: 18px;
                margin: 2px;
            }}
            QSpinBox::up-button:hover {{
                background-color: {COLORS["accent_hover"]};
            }}
            QSpinBox::down-button:hover {{
                background-color: {COLORS["error_hover"]};
            }}
            QSpinBox::up-arrow {{
                image: none;
                width: 0; height: 0;
            }}
            QSpinBox::down-arrow {{
                image: none;
                width: 0; height: 0;
            }}
        """)


class StyledDoubleSpinBox(QDoubleSpinBox):
    """Custom styled double spin box."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setButtonSymbols(QDoubleSpinBox.ButtonSymbols.PlusMinus)
        self.setStyleSheet(f"""
            QDoubleSpinBox {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 13px;
                min-width: 70px;
            }}
            QDoubleSpinBox:focus {{
                border-color: {COLORS["accent"]};
            }}
            QDoubleSpinBox::up-button {{
                background-color: {COLORS["accent"]};
                border: none;
                border-radius: 2px;
                width: 16px;
                margin: 2px;
            }}
            QDoubleSpinBox::down-button {{
                background-color: {COLORS["error"]};
                border: none;
                border-radius: 2px;
                width: 16px;
                margin: 2px;
            }}
            QDoubleSpinBox::up-arrow, QDoubleSpinBox::down-arrow {{
                image: none;
                width: 0; height: 0;
            }}
        """)


class StyledComboBox(QComboBox):
    """Custom styled combo box."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"""
            QComboBox {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 6px;
                padding: 8px;
                font-size: 14px;
                min-width: 120px;
            }}
            QComboBox:focus {{
                border-color: {COLORS["accent"]};
            }}
            QComboBox::drop-down {{
                border: none;
                width: 30px;
            }}
            QComboBox::down-arrow {{
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid {COLORS["text"]};
            }}
            QComboBox QAbstractItemView {{
                background-color: {COLORS["surface"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                selection-background-color: {COLORS["accent"]};
            }}
        """)


class StyledLineEdit(QLineEdit):
    """Custom styled line edit."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"""
            QLineEdit {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 6px;
                padding: 8px;
                font-size: 14px;
            }}
            QLineEdit:focus {{
                border-color: {COLORS["accent"]};
            }}
        """)


class SliderSpinBox(QWidget):
    """Combined slider and spinbox widget for numeric input."""
    
    valueChanged = pyqtSignal(int)
    
    def __init__(self, min_val: int = 0, max_val: int = 100, default: int = 50, step: int = 1, parent=None):
        super().__init__(parent)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # Slider
        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.setRange(min_val, max_val)
        self.slider.setValue(default)
        self.slider.setSingleStep(step)
        self.slider.setStyleSheet(f"""
            QSlider::groove:horizontal {{
                background: {COLORS["surface_light"]};
                height: 6px;
                border-radius: 3px;
            }}
            QSlider::handle:horizontal {{
                background: {COLORS["accent"]};
                width: 16px;
                height: 16px;
                margin: -5px 0;
                border-radius: 8px;
            }}
            QSlider::handle:horizontal:hover {{
                background: {COLORS["accent_hover"]};
            }}
            QSlider::sub-page:horizontal {{
                background: {COLORS["accent"]};
                border-radius: 3px;
            }}
        """)
        layout.addWidget(self.slider, stretch=3)
        
        # SpinBox
        self.spinbox = StyledSpinBox()
        self.spinbox.setRange(min_val, max_val)
        self.spinbox.setValue(default)
        self.spinbox.setSingleStep(step)
        layout.addWidget(self.spinbox, stretch=1)
        
        # Connect signals
        self.slider.valueChanged.connect(self._on_slider_change)
        self.spinbox.valueChanged.connect(self._on_spinbox_change)
    
    def _on_slider_change(self, value):
        self.spinbox.blockSignals(True)
        self.spinbox.setValue(value)
        self.spinbox.blockSignals(False)
        self.valueChanged.emit(value)
    
    def _on_spinbox_change(self, value):
        self.slider.blockSignals(True)
        self.slider.setValue(value)
        self.slider.blockSignals(False)
        self.valueChanged.emit(value)
    
    def value(self) -> int:
        return self.spinbox.value()
    
    def setValue(self, val: int):
        self.spinbox.setValue(val)
        self.slider.setValue(val)
    
    def setRange(self, min_val: int, max_val: int):
        self.slider.setRange(min_val, max_val)
        self.spinbox.setRange(min_val, max_val)


class TelcoDataGeneratorGUI(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        self.worker = None
        self._fabric_credential = None  # Store authenticated Fabric credential
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle(f"🇳🇱 Telco Customer Data Generator v{__version__}")
        self.setMinimumSize(1100, 750)
        
        # Set application style
        self.setStyleSheet(f"""
            QMainWindow {{
                background-color: {COLORS["background"]};
            }}
            QWidget {{
                color: {COLORS["text"]};
                font-family: 'Segoe UI', 'SF Pro Display', -apple-system, sans-serif;
            }}
            QLabel {{
                color: {COLORS["text"]};
            }}
            QScrollArea {{
                border: none;
                background-color: transparent;
            }}
            QScrollArea > QWidget > QWidget {{
                background-color: transparent;
            }}
            QScrollBar:vertical {{
                background-color: {COLORS["surface"]};
                width: 10px;
                border-radius: 5px;
                margin: 2px;
            }}
            QScrollBar::handle:vertical {{
                background-color: {COLORS["border"]};
                border-radius: 5px;
                min-height: 30px;
            }}
            QScrollBar::handle:vertical:hover {{
                background-color: {COLORS["text_muted"]};
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0;
            }}
            QTabWidget::pane {{
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
                background-color: {COLORS["surface"]};
                margin-top: -1px;
            }}
            QTabBar {{
                background: transparent;
            }}
            QTabBar::tab {{
                background-color: {COLORS["surface_light"]};
                color: {COLORS["text_muted"]};
                padding: 12px 24px;
                margin-right: 4px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                font-size: 13px;
                font-weight: 500;
                border: 1px solid {COLORS["border"]};
                border-bottom: none;
            }}
            QTabBar::tab:selected {{
                background-color: {COLORS["surface"]};
                color: {COLORS["accent"]};
                font-weight: 600;
            }}
            QTabBar::tab:hover:!selected {{
                background-color: {COLORS["surface_hover"]};
                color: {COLORS["text"]};
            }}
            QProgressBar {{
                background-color: {COLORS["surface_light"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 8px;
                height: 24px;
                text-align: center;
                color: {COLORS["text"]};
                font-weight: 500;
            }}
            QProgressBar::chunk {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["gradient_start"]}, stop:1 {COLORS["gradient_end"]});
                border-radius: 7px;
            }}
            QTextEdit {{
                background-color: {COLORS["surface"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 8px;
                font-family: 'Cascadia Code', 'Consolas', 'Monaco', monospace;
                font-size: 12px;
                padding: 8px;
                selection-background-color: {COLORS["accent_dim"]};
            }}
            QToolTip {{
                background-color: {COLORS["surface"]};
                color: {COLORS["text"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 4px;
                padding: 6px 10px;
                font-size: 12px;
            }}
        """)
        
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)
        
        # Header
        header = self._create_header()
        main_layout.addWidget(header)
        
        # Main content area with tabs
        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        
        # Configuration tab
        config_tab = self._create_config_tab()
        tabs.addTab(config_tab, "⚙️  Data Generation")
        
        # Advanced settings tab
        advanced_tab = self._create_advanced_tab()
        tabs.addTab(advanced_tab, "🔧  Advanced")
        
        # Fabric tab
        fabric_tab = self._create_fabric_tab()
        tabs.addTab(fabric_tab, "☁️  Microsoft Fabric")
        
        # Output tab
        output_tab = self._create_output_tab()
        tabs.addTab(output_tab, "📋  Output Log")
        
        main_layout.addWidget(tabs)
    
    def _create_header(self) -> QWidget:
        """Create a rich header with title, status, and presets."""
        header = QWidget()
        header.setStyleSheet(f"""
            QWidget {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["surface"]}, stop:1 {COLORS["surface_light"]});
                border-radius: 12px;
                border: 1px solid {COLORS["border"]};
            }}
        """)
        layout = QHBoxLayout(header)
        layout.setContentsMargins(16, 12, 16, 12)
        
        # Logo and Title
        title_section = QVBoxLayout()
        title_section.setSpacing(2)
        
        title = QLabel("🇳🇱 Dutch Telco Data Generator")
        title.setFont(QFont("Segoe UI", 16, QFont.Weight.Bold))
        title.setStyleSheet(f"color: {COLORS['accent']}; background: transparent; border: none;")
        title_section.addWidget(title)
        
        subtitle = QLabel("Generate realistic Netherlands telecommunications customer data")
        subtitle.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 11px; background: transparent; border: none;")
        title_section.addWidget(subtitle)
        
        layout.addLayout(title_section)
        layout.addStretch()
        
        # Preset section
        preset_label = QLabel("Quick Presets:")
        preset_label.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 12px; background: transparent; border: none; margin-right: 8px;")
        layout.addWidget(preset_label)
        
        presets = [
            ("🔹 Small", "100 parties", 1, "default"),
            ("🔷 Medium", "1K parties", 2, "info"),
            ("🔶 Large", "10K parties", 3, "orange"),
            ("💎 Enterprise", "100K parties", 4, "purple"),
        ]
        
        for name, tooltip, idx, color in presets:
            btn = StyledButton(name, color)
            btn.setToolTip(tooltip)
            btn.setStyleSheet(btn.styleSheet() + """
                QPushButton { 
                    padding: 6px 12px; 
                    font-size: 11px; 
                    font-weight: 600;
                    border-radius: 6px;
                }
            """)
            btn.clicked.connect(lambda checked, i=idx: self._apply_preset(i))
            layout.addWidget(btn)
        
        return header
    
    def _create_config_tab(self) -> QWidget:
        """Create the configuration tab with organized sections."""
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        tab_layout.setSpacing(12)
        tab_layout.setContentsMargins(12, 12, 12, 12)

        # Content area (horizontal two-column)
        layout = QHBoxLayout()
        layout.setSpacing(16)
        
        # Left column - Core entities
        left_col = QVBoxLayout()
        left_col.setSpacing(12)
        
        # Core entities group
        core_group = StyledGroupBox("Core Entities", "👥")
        core_layout = QGridLayout(core_group)
        core_layout.setSpacing(12)
        core_layout.setContentsMargins(12, 12, 12, 12)
        
        row = 0
        # Parties
        parties_label = QLabel("👤 Parties:")
        parties_label.setToolTip("Number of customers/parties to generate")
        core_layout.addWidget(parties_label, row, 0)
        self.parties_spin = SliderSpinBox(10, 100000, 1000, 100)
        core_layout.addWidget(self.parties_spin, row, 1)
        
        row += 1
        # Accounts
        accounts_label = QLabel("📋 Accounts:")
        accounts_label.setToolTip("Number of billing accounts")
        core_layout.addWidget(accounts_label, row, 0)
        self.accounts_spin = SliderSpinBox(10, 100000, 800, 100)
        core_layout.addWidget(self.accounts_spin, row, 1)
        
        row += 1
        # Subscribers
        subscribers_label = QLabel("📱 Subscribers:")
        subscribers_label.setToolTip("Number of mobile/service subscribers")
        core_layout.addWidget(subscribers_label, row, 0)
        self.subscribers_spin = SliderSpinBox(10, 150000, 1200, 100)
        core_layout.addWidget(self.subscribers_spin, row, 1)
        
        row += 1
        # Products
        products_label = QLabel("📦 Products:")
        products_label.setToolTip("Number of product types in catalog")
        core_layout.addWidget(products_label, row, 0)
        self.products_spin = SliderSpinBox(5, 200, 50, 5)
        core_layout.addWidget(self.products_spin, row, 1)
        
        left_col.addWidget(core_group)
        
        # Output settings group
        output_group = StyledGroupBox("Output Settings", "💾")
        output_layout = QGridLayout(output_group)
        output_layout.setSpacing(12)
        output_layout.setContentsMargins(12, 12, 12, 12)
        
        row = 0
        # Output directory
        output_layout.addWidget(QLabel("📁 Directory:"), row, 0)
        dir_layout = QHBoxLayout()
        self.output_dir_edit = StyledLineEdit()
        self.output_dir_edit.setText("output")
        self.output_dir_edit.setPlaceholderText("Select output folder...")
        dir_layout.addWidget(self.output_dir_edit)
        browse_btn = StyledButton("📂 Browse", "default")
        browse_btn.clicked.connect(self._browse_output_dir)
        dir_layout.addWidget(browse_btn)
        output_layout.addLayout(dir_layout, row, 1)
        
        row += 1
        # Output format
        output_layout.addWidget(QLabel("📄 Format:"), row, 0)
        self.format_combo = StyledComboBox()
        self.format_combo.addItems(["CSV", "JSON", "Parquet"])
        output_layout.addWidget(self.format_combo, row, 1)
        
        row += 1
        # Random seed
        output_layout.addWidget(QLabel("🎲 Seed:"), row, 0)
        seed_layout = QHBoxLayout()
        self.seed_spin = StyledSpinBox()
        self.seed_spin.setRange(0, 999999)
        self.seed_spin.setValue(42)
        self.seed_spin.setToolTip("Random seed for reproducible data")
        seed_layout.addWidget(self.seed_spin)
        self.use_seed_check = QCheckBox("Use seed")
        self.use_seed_check.setChecked(True)
        self.use_seed_check.setStyleSheet(f"color: {COLORS['text']}; font-size: 12px;")
        seed_layout.addWidget(self.use_seed_check)
        output_layout.addLayout(seed_layout, row, 1)
        
        left_col.addWidget(output_group)
        left_col.addStretch()
        
        layout.addLayout(left_col, stretch=1)
        
        # Right column - Ratios + Estimate
        right_col = QVBoxLayout()
        right_col.setSpacing(12)
        
        ratios_group = StyledGroupBox("Data Ratios", "⚖️")
        ratios_layout = QGridLayout(ratios_group)
        ratios_layout.setSpacing(10)
        ratios_layout.setContentsMargins(12, 12, 12, 12)
        
        ratio_fields = [
            ("📊 Subscriptions/Subscriber:", "subscriptions_ratio", 0.5, 10.0, 1.5, 0.1),
            ("🧾 Invoices/Account:", "invoices_ratio", 1.0, 60.0, 12.0, 1.0),
            ("💳 Charges/Account:", "charges_ratio", 1.0, 100.0, 36.0, 1.0),
            ("💰 Payments/Account:", "payments_ratio", 1.0, 50.0, 10.0, 1.0),
            ("🎫 Tickets/Account:", "tickets_ratio", 0.0, 20.0, 2.0, 0.5),
            ("📲 Prepaid Ratio:", "prepaid_ratio", 0.0, 1.0, 0.3, 0.05),
        ]
        
        for row, (label, attr, min_v, max_v, default, step) in enumerate(ratio_fields):
            ratios_layout.addWidget(QLabel(label), row, 0)
            spin = StyledDoubleSpinBox()
            spin.setRange(min_v, max_v)
            spin.setValue(default)
            spin.setSingleStep(step)
            setattr(self, attr, spin)
            ratios_layout.addWidget(spin, row, 1)
        
        right_col.addWidget(ratios_group, stretch=1)
        
        # Estimate card
        estimate_card = QFrame()
        estimate_card.setStyleSheet(f"""
            QFrame {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["surface"]}, stop:1 {COLORS["surface_light"]});
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
                padding: 12px;
            }}
        """)
        estimate_layout = QHBoxLayout(estimate_card)
        estimate_layout.setContentsMargins(16, 12, 16, 12)
        
        estimate_icon = QLabel("📊")
        estimate_icon.setStyleSheet("font-size: 24px; background: transparent;")
        estimate_layout.addWidget(estimate_icon)
        
        estimate_text_layout = QVBoxLayout()
        estimate_text_layout.setSpacing(2)
        estimate_title = QLabel("Estimated Records")
        estimate_title.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 11px; background: transparent;")
        estimate_text_layout.addWidget(estimate_title)
        
        self.estimate_label = QLabel("~30,000")
        self.estimate_label.setStyleSheet(f"color: {COLORS['accent']}; font-size: 20px; font-weight: bold; background: transparent;")
        estimate_text_layout.addWidget(self.estimate_label)
        estimate_layout.addLayout(estimate_text_layout)
        estimate_layout.addStretch()
        
        right_col.addWidget(estimate_card)
        
        layout.addLayout(right_col, stretch=1)
        
        # Connect signals for estimate updates
        self.parties_spin.valueChanged.connect(self._update_estimate)
        self.accounts_spin.valueChanged.connect(self._update_estimate)
        self.subscribers_spin.valueChanged.connect(self._update_estimate)
        self.invoices_ratio.valueChanged.connect(self._update_estimate)
        self.charges_ratio.valueChanged.connect(self._update_estimate)
        
        tab_layout.addLayout(layout, 1)

        # Action buttons row at bottom
        action_card = QFrame()
        action_card.setStyleSheet(f"""
            QFrame {{
                background-color: {COLORS["surface"]};
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
                padding: 8px;
            }}
        """)
        action_row = QHBoxLayout(action_card)
        action_row.setContentsMargins(12, 8, 12, 8)
        action_row.setSpacing(10)

        # Left side - utility buttons
        load_config_btn = StyledButton("📥 Load Config", "default")
        load_config_btn.clicked.connect(self._load_config)
        action_row.addWidget(load_config_btn)

        save_config_btn = StyledButton("💾 Save Config", "default")
        save_config_btn.clicked.connect(self._save_config)
        action_row.addWidget(save_config_btn)

        self.open_folder_btn = StyledButton("📂 Open Output", "info")
        self.open_folder_btn.clicked.connect(self._open_output_folder)
        action_row.addWidget(self.open_folder_btn)

        action_row.addStretch()

        # Right side - main actions
        self.stop_btn = StyledButton("⏹️ Stop", "error")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._stop_generation)
        action_row.addWidget(self.stop_btn)

        self.generate_btn = StyledButton("▶️  Generate Data", "success")
        self.generate_btn.setMinimumWidth(180)
        self.generate_btn.setStyleSheet(self.generate_btn.styleSheet() + """
            QPushButton { 
                font-size: 14px; 
                padding: 12px 24px;
                font-weight: bold;
            }
        """)
        self.generate_btn.clicked.connect(self._start_generation)
        action_row.addWidget(self.generate_btn)

        tab_layout.addWidget(action_card)
        
        return tab
    
    def _create_advanced_tab(self) -> QWidget:
        """Create the advanced settings tab with organized sections."""
        tab = QWidget()
        layout = QHBoxLayout(tab)
        layout.setSpacing(16)
        layout.setContentsMargins(12, 12, 12, 12)
        
        # Left column
        left_col = QVBoxLayout()
        left_col.setSpacing(12)
        
        # Date range group
        date_group = StyledGroupBox("Date Range", "📅")
        date_layout = QGridLayout(date_group)
        date_layout.setSpacing(12)
        date_layout.setContentsMargins(12, 12, 12, 12)
        
        date_layout.addWidget(QLabel("📆 Start Year:"), 0, 0)
        self.start_year_spin = StyledSpinBox()
        self.start_year_spin.setRange(2015, 2030)
        self.start_year_spin.setValue(2020)
        date_layout.addWidget(self.start_year_spin, 0, 1)
        
        date_layout.addWidget(QLabel("📆 End Year:"), 1, 0)
        self.end_year_spin = StyledSpinBox()
        self.end_year_spin.setRange(2015, 2030)
        self.end_year_spin.setValue(2026)
        date_layout.addWidget(self.end_year_spin, 1, 1)
        
        left_col.addWidget(date_group)
        
        # Operator settings group
        operator_group = StyledGroupBox("Operator Settings", "🏢")
        operator_layout = QGridLayout(operator_group)
        operator_layout.setSpacing(12)
        operator_layout.setContentsMargins(12, 12, 12, 12)
        
        operator_layout.addWidget(QLabel("🏷️ Name:"), 0, 0)
        self.operator_edit = StyledLineEdit()
        self.operator_edit.setText("BrightTelco")
        self.operator_edit.setPlaceholderText("Operator name...")
        operator_layout.addWidget(self.operator_edit, 0, 1)

        operator_layout.addWidget(QLabel("🏪 Brands:"), 1, 0)
        self.brands_edit = StyledLineEdit()
        self.brands_edit.setText("BrightTelco, npkNL, BrightMobile")
        self.brands_edit.setPlaceholderText("Comma-separated brands...")
        operator_layout.addWidget(self.brands_edit, 1, 1)
        
        left_col.addWidget(operator_group)
        left_col.addStretch()
        
        layout.addLayout(left_col)
        
        # Right column
        right_col = QVBoxLayout()
        right_col.setSpacing(12)
        
        # Additional ratios group
        more_ratios_group = StyledGroupBox("Additional Ratios", "📈")
        more_ratios_layout = QGridLayout(more_ratios_group)
        more_ratios_layout.setSpacing(12)
        more_ratios_layout.setContentsMargins(12, 12, 12, 12)
        
        extra_ratios = [
            ("📱 Devices/Subscriber:", "devices_ratio", 0.5, 5.0, 1.2, 0.1),
            ("🛒 Orders/Subscriber:", "orders_ratio", 0.5, 10.0, 3.0, 0.5),
            ("🔄 Porting Ratio:", "porting_ratio", 0.0, 1.0, 0.15, 0.05),
            ("🎁 Entitlements/Sub:", "entitlements_ratio", 0.5, 10.0, 2.0, 0.5),
        ]
        
        for row, (label, attr, min_v, max_v, default, step) in enumerate(extra_ratios):
            more_ratios_layout.addWidget(QLabel(label), row, 0)
            spin = StyledDoubleSpinBox()
            spin.setRange(min_v, max_v)
            spin.setValue(default)
            spin.setSingleStep(step)
            setattr(self, attr, spin)
            more_ratios_layout.addWidget(spin, row, 1)
        
        right_col.addWidget(more_ratios_group)
        right_col.addStretch()
        
        layout.addLayout(right_col)
        
        return tab
    
    def _create_fabric_tab(self) -> QWidget:
        """Create the Microsoft Fabric configuration tab with workflow steps."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        # Step 1: Connection
        step1 = QHBoxLayout()
        step1.setSpacing(12)
        
        conn_group = StyledGroupBox("Step 1: Connect", "🔐")
        conn_layout = QGridLayout(conn_group)
        conn_layout.setSpacing(8)
        conn_layout.setContentsMargins(12, 16, 12, 12)

        conn_layout.addWidget(QLabel("🔑 Auth Method:"), 0, 0)
        self.fabric_auth_combo = StyledComboBox()
        self.fabric_auth_combo.addItems(["Browser Login", "Device Code"])
        conn_layout.addWidget(self.fabric_auth_combo, 0, 1)

        status_row = QHBoxLayout()
        self.fabric_status_label = QLabel("⭕ Not connected")
        self.fabric_status_label.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 12px;")
        status_row.addWidget(self.fabric_status_label)
        status_row.addStretch()
        test_conn_btn = StyledButton("🔗 Test Connection", "info")
        test_conn_btn.clicked.connect(self._test_fabric_connection)
        status_row.addWidget(test_conn_btn)
        conn_layout.addLayout(status_row, 1, 0, 1, 2)
        
        step1.addWidget(conn_group, 1)

        # Capacity section
        cap_group = StyledGroupBox("Fabric Capacity", "⚡")
        cap_layout = QVBoxLayout(cap_group)
        cap_layout.setSpacing(8)
        cap_layout.setContentsMargins(12, 16, 12, 12)

        cap_combo_row = QHBoxLayout()
        self.fabric_capacity_combo = StyledComboBox()
        self.fabric_capacity_combo.addItem("-- Select capacity --", "")
        cap_combo_row.addWidget(self.fabric_capacity_combo, 1)
        self.fabric_refresh_cap_btn = StyledButton("🔄", "default")
        self.fabric_refresh_cap_btn.setToolTip("Refresh capacities")
        self.fabric_refresh_cap_btn.setMaximumWidth(40)
        self.fabric_refresh_cap_btn.clicked.connect(self._refresh_capacities)
        cap_combo_row.addWidget(self.fabric_refresh_cap_btn)
        cap_layout.addLayout(cap_combo_row)

        self.fabric_capacity_info = QLabel("💡 Connect first, then select capacity")
        self.fabric_capacity_info.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 11px;")
        cap_layout.addWidget(self.fabric_capacity_info)
        
        step1.addWidget(cap_group, 1)
        layout.addLayout(step1)

        # Step 2: Workspace & Lakehouse
        step2 = QHBoxLayout()
        step2.setSpacing(12)
        
        ws_group = StyledGroupBox("Step 2: Workspace & Lakehouse", "🗂️")
        ws_layout = QGridLayout(ws_group)
        ws_layout.setSpacing(8)
        ws_layout.setContentsMargins(12, 16, 12, 12)

        ws_layout.addWidget(QLabel("📁 Workspace:"), 0, 0)
        self.fabric_workspace_edit = StyledLineEdit()
        self.fabric_workspace_edit.setText("Teclo-E2E-Demo")
        self.fabric_workspace_edit.setPlaceholderText("Workspace name...")
        ws_layout.addWidget(self.fabric_workspace_edit, 0, 1)

        ws_layout.addWidget(QLabel("🏠 Lakehouse:"), 1, 0)
        self.fabric_lakehouse_edit = StyledLineEdit()
        self.fabric_lakehouse_edit.setText("telco_data")
        self.fabric_lakehouse_edit.setPlaceholderText("Lakehouse name...")
        ws_layout.addWidget(self.fabric_lakehouse_edit, 1, 1)

        self.fabric_auto_create_check = QCheckBox("✨ Auto-create if not exists")
        self.fabric_auto_create_check.setChecked(True)
        self.fabric_auto_create_check.setStyleSheet(f"color: {COLORS['text']}; font-size: 12px;")
        ws_layout.addWidget(self.fabric_auto_create_check, 2, 0, 1, 2)
        
        step2.addWidget(ws_group, 1)

        # Data Source section
        data_group = StyledGroupBox("Step 3: Data Source", "📂")
        data_layout = QGridLayout(data_group)
        data_layout.setSpacing(8)
        data_layout.setContentsMargins(12, 16, 12, 12)

        data_layout.addWidget(QLabel("📁 Local Dir:"), 0, 0)
        src_row = QHBoxLayout()
        self.fabric_source_dir = StyledLineEdit()
        self.fabric_source_dir.setText("output")
        self.fabric_source_dir.setPlaceholderText("Path to generated data...")
        src_row.addWidget(self.fabric_source_dir, 1)
        browse_btn = StyledButton("📂", "default")
        browse_btn.setToolTip("Browse for folder")
        browse_btn.setMaximumWidth(40)
        browse_btn.clicked.connect(self._browse_fabric_source_dir)
        src_row.addWidget(browse_btn)
        data_layout.addLayout(src_row, 0, 1)

        data_layout.addWidget(QLabel("☁️ Remote Path:"), 1, 0)
        self.fabric_remote_folder = StyledLineEdit()
        self.fabric_remote_folder.setText("Files/telco_data")
        self.fabric_remote_folder.setPlaceholderText("OneLake path...")
        data_layout.addWidget(self.fabric_remote_folder, 1, 1)
        
        step2.addWidget(data_group, 1)
        layout.addLayout(step2)

        # Step 4: Upload action card
        upload_card = QFrame()
        upload_card.setStyleSheet(f"""
            QFrame {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["surface"]}, stop:1 {COLORS["surface_light"]});
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
            }}
        """)
        upload_layout = QHBoxLayout(upload_card)
        upload_layout.setContentsMargins(16, 12, 16, 12)
        upload_layout.setSpacing(12)
        
        # Step indicator
        step_label = QLabel("Step 4:")
        step_label.setStyleSheet(f"color: {COLORS['accent']}; font-weight: bold; font-size: 13px; background: transparent;")
        upload_layout.addWidget(step_label)
        
        # Progress bar
        self.fabric_progress = QProgressBar()
        self.fabric_progress.setValue(0)
        self.fabric_progress.setFormat("%p%")
        self.fabric_progress.setFixedHeight(24)
        self.fabric_progress.setMinimumWidth(200)
        upload_layout.addWidget(self.fabric_progress, 1)

        # Upload button
        self.fabric_upload_btn = StyledButton("🚀 Upload to Fabric", "success")
        self.fabric_upload_btn.setStyleSheet(self.fabric_upload_btn.styleSheet() + """
            QPushButton { 
                padding: 10px 24px; 
                font-size: 13px; 
                font-weight: bold;
            }
        """)
        self.fabric_upload_btn.clicked.connect(self._start_fabric_upload)
        upload_layout.addWidget(self.fabric_upload_btn)

        # Stop button
        self.fabric_stop_btn = StyledButton("⏹️ Stop", "error")
        self.fabric_stop_btn.setEnabled(False)
        self.fabric_stop_btn.clicked.connect(self._stop_fabric_upload)
        upload_layout.addWidget(self.fabric_stop_btn)
        
        layout.addWidget(upload_card)

        # Log output
        log_label = QLabel("📋 Upload Log:")
        log_label.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 12px; margin-top: 8px;")
        layout.addWidget(log_label)
        
        self.fabric_log = QTextEdit()
        self.fabric_log.setReadOnly(True)
        self.fabric_log.setFont(QFont("Cascadia Code", 10))
        self.fabric_log.setMaximumHeight(120)
        self.fabric_log.setStyleSheet(f"""
            QTextEdit {{
                background-color: {COLORS['surface']};
                color: {COLORS['text']};
                border: 1px solid {COLORS['border']};
                border-radius: 8px;
                padding: 8px;
            }}
        """)
        self.fabric_log.setPlaceholderText("Upload progress will appear here...")
        layout.addWidget(self.fabric_log)

        return tab
    
    def _create_output_tab(self) -> QWidget:
        """Create the output log tab with clean layout."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)
        
        # Status card
        status_card = QFrame()
        status_card.setStyleSheet(f"""
            QFrame {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 {COLORS["surface"]}, stop:1 {COLORS["surface_light"]});
                border: 1px solid {COLORS["border"]};
                border-radius: 10px;
            }}
        """)
        status_layout = QHBoxLayout(status_card)
        status_layout.setContentsMargins(16, 12, 16, 12)
        
        # Status icon and text
        status_icon = QLabel("📊")
        status_icon.setStyleSheet("font-size: 24px; background: transparent;")
        status_layout.addWidget(status_icon)
        
        status_text_layout = QVBoxLayout()
        status_text_layout.setSpacing(2)
        
        self.status_label = QLabel("Ready to generate")
        self.status_label.setStyleSheet(f"color: {COLORS['text']}; font-size: 14px; font-weight: 500; background: transparent;")
        status_text_layout.addWidget(self.status_label)
        
        status_layout.addLayout(status_text_layout)
        status_layout.addStretch()
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setMinimumWidth(300)
        self.progress_bar.setFixedHeight(24)
        status_layout.addWidget(self.progress_bar)
        
        layout.addWidget(status_card)
        
        # Log header
        log_header = QHBoxLayout()
        log_title = QLabel("📋 Generation Log")
        log_title.setStyleSheet(f"color: {COLORS['text']}; font-size: 13px; font-weight: 600;")
        log_header.addWidget(log_title)
        log_header.addStretch()
        
        clear_btn = StyledButton("🗑️ Clear", "default")
        clear_btn.clicked.connect(self._clear_log)
        log_header.addWidget(clear_btn)
        
        layout.addLayout(log_header)
        
        # Log output
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFont(QFont("Cascadia Code", 11))
        self.log_output.setStyleSheet(f"""
            QTextEdit {{
                background-color: {COLORS['surface']};
                color: {COLORS['text']};
                border: 1px solid {COLORS['border']};
                border-radius: 8px;
                padding: 12px;
                line-height: 1.4;
            }}
        """)
        self.log_output.setPlaceholderText("Generation logs will appear here...")
        layout.addWidget(self.log_output)
        
        return tab
    
    def _apply_preset(self, index: int):
        """Apply a preset configuration."""
        presets = {
            1: (100, 80, 120, 20),      # Small
            2: (1000, 800, 1200, 50),   # Medium
            3: (10000, 8000, 12000, 100),  # Large
            4: (100000, 80000, 150000, 200),  # Enterprise
        }
        
        if index in presets:
            parties, accounts, subscribers, products = presets[index]
            self.parties_spin.setValue(parties)
            self.accounts_spin.setValue(accounts)
            self.subscribers_spin.setValue(subscribers)
            self.products_spin.setValue(products)
            self._update_estimate()
    
    def _update_estimate(self):
        """Update the estimated record count."""
        parties = self.parties_spin.value()
        accounts = self.accounts_spin.value()
        subscribers = self.subscribers_spin.value()
        invoices_ratio = self.invoices_ratio.value()
        charges_ratio = self.charges_ratio.value()
        
        # Rough estimate calculation
        estimate = (
            parties * 3 +  # party, address, party_address
            accounts * 2 +  # account, account_party_role
            subscribers * 8 +  # subscriber, msisdn, sim, device, status_history, etc.
            int(accounts * invoices_ratio * 4.5) +  # invoices, invoice_lines
            int(accounts * charges_ratio) +  # charges
            int(accounts * self.payments_ratio.value()) +  # payments
            int(accounts * self.tickets_ratio.value())  # tickets
        )
        
        if estimate < 1000:
            estimate_str = f"~{estimate:,} records (26 tables)"
        elif estimate < 1000000:
            estimate_str = f"~{estimate // 1000}K records (26 tables)"
        else:
            estimate_str = f"~{estimate / 1000000:.1f}M records (26 tables)"
        
        self.estimate_label.setText(estimate_str)
    
    def _browse_output_dir(self):
        """Open directory browser for output directory."""
        dir_path = QFileDialog.getExistingDirectory(
            self,
            "Select Output Directory",
            self.output_dir_edit.text()
        )
        if dir_path:
            self.output_dir_edit.setText(dir_path)
    
    def _get_config(self) -> GeneratorConfig:
        """Build configuration from UI values."""
        brands = [b.strip() for b in self.brands_edit.text().split(",") if b.strip()]
        
        return GeneratorConfig(
            num_parties=self.parties_spin.value(),
            num_accounts=self.accounts_spin.value(),
            num_subscribers=self.subscribers_spin.value(),
            num_products=self.products_spin.value(),
            output_dir=self.output_dir_edit.text(),
            output_format=self.format_combo.currentText().lower(),
            random_seed=self.seed_spin.value() if self.use_seed_check.isChecked() else None,
            subscriptions_per_subscriber=self.subscriptions_ratio.value(),
            invoices_per_account=self.invoices_ratio.value(),
            charges_per_account=self.charges_ratio.value(),
            payments_per_account=self.payments_ratio.value(),
            tickets_per_account=self.tickets_ratio.value(),
            prepaid_ratio=self.prepaid_ratio.value(),
            data_start_year=self.start_year_spin.value(),
            data_end_year=self.end_year_spin.value(),
            operator_name=self.operator_edit.text(),
            brands=brands,
            devices_per_subscriber=self.devices_ratio.value(),
            orders_per_subscriber=self.orders_ratio.value(),
            porting_ratio=self.porting_ratio.value(),
            entitlements_per_subscription=self.entitlements_ratio.value(),
        )
    
    def _set_config(self, config: GeneratorConfig):
        """Set UI values from configuration."""
        self.parties_spin.setValue(config.num_parties)
        self.accounts_spin.setValue(config.num_accounts)
        self.subscribers_spin.setValue(config.num_subscribers)
        self.products_spin.setValue(config.num_products)
        self.output_dir_edit.setText(config.output_dir)
        self.format_combo.setCurrentText(config.output_format.upper())
        if config.random_seed:
            self.seed_spin.setValue(config.random_seed)
            self.use_seed_check.setChecked(True)
        else:
            self.use_seed_check.setChecked(False)
        self.subscriptions_ratio.setValue(config.subscriptions_per_subscriber)
        self.invoices_ratio.setValue(config.invoices_per_account)
        self.charges_ratio.setValue(config.charges_per_account)
        self.payments_ratio.setValue(config.payments_per_account)
        self.tickets_ratio.setValue(config.tickets_per_account)
        self.prepaid_ratio.setValue(config.prepaid_ratio)
        self.start_year_spin.setValue(config.data_start_year)
        self.end_year_spin.setValue(config.data_end_year)
        self.operator_edit.setText(config.operator_name)
        self.brands_edit.setText(", ".join(config.brands))
        self.devices_ratio.setValue(config.devices_per_subscriber)
        self.orders_ratio.setValue(config.orders_per_subscriber)
        self.porting_ratio.setValue(config.porting_ratio)
        self.entitlements_ratio.setValue(config.entitlements_per_subscription)
        
        self._update_estimate()
    
    def _load_config(self):
        """Load configuration from file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Load Configuration",
            "",
            "JSON Files (*.json)"
        )
        if file_path:
            try:
                config = GeneratorConfig.from_json_file(file_path)
                self._set_config(config)
                self._log(f"Loaded configuration from {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load config: {e}")
    
    def _save_config(self):
        """Save configuration to file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Configuration",
            "config.json",
            "JSON Files (*.json)"
        )
        if file_path:
            try:
                config = self._get_config()
                config.to_json_file(file_path)
                self._log(f"Saved configuration to {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save config: {e}")
    
    def _start_generation(self):
        """Start the data generation process."""
        config = self._get_config()
        
        # Validate
        try:
            config.validate()
        except AssertionError as e:
            QMessageBox.warning(self, "Invalid Configuration", str(e))
            return
        
        # Update UI
        self.generate_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_bar.setValue(0)
        
        self._log("=" * 60)
        self._log(f"Starting data generation...")
        self._log(f"Parties: {config.num_parties}, Accounts: {config.num_accounts}, Subscribers: {config.num_subscribers}")
        self._log("=" * 60)
        
        # Start worker
        self.worker = GeneratorWorker(config)
        self.worker.progress_updated.connect(self._on_progress)
        self.worker.generation_complete.connect(self._on_complete)
        self.worker.generation_error.connect(self._on_error)
        self.worker.start()
    
    def _stop_generation(self):
        """Stop the data generation process."""
        if self.worker:
            self.worker.stop()
            self.worker.wait()
            self.worker = None
        
        self.generate_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._log("Generation stopped by user")
    
    def _on_progress(self, message: str, percentage: int):
        """Handle progress updates."""
        self.progress_bar.setValue(percentage)
        self.status_label.setText(message)
        self._log(message)
    
    def _on_complete(self, summary: dict):
        """Handle generation complete."""
        self.generate_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        
        # Update Fabric source directory to point to generated data
        self.fabric_source_dir.setText(summary['output_dir'])
        
        self._log("=" * 60)
        self._log("Generation complete!")
        self._log(f"Total records: {summary['total_records']:,}")
        self._log(f"Output directory: {summary['output_dir']}")
        self._log(f"Format: {summary['format'].upper()}")
        self._log("-" * 40)
        self._log("Records per table:")
        for table, count in sorted(summary['tables'].items()):
            if count > 0:
                self._log(f"  {table}: {count:,}")
        self._log("=" * 60)
        
        self.status_label.setText(f"Complete! Generated {summary['total_records']:,} records")
        
        QMessageBox.information(
            self,
            "Generation Complete",
            f"Successfully generated {summary['total_records']:,} records\n\n"
            f"Output saved to: {summary['output_dir']}"
        )
    
    def _on_error(self, error_message: str):
        """Handle generation error."""
        self.generate_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        
        self._log(f"ERROR: {error_message}")
        self.status_label.setText("Error occurred")
        
        QMessageBox.critical(self, "Generation Error", error_message)
    
    def _log(self, message: str):
        """Add message to log output."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.append(f"[{timestamp}] {message}")
        self.log_output.moveCursor(QTextCursor.MoveOperation.End)
    
    def _clear_log(self):
        """Clear the log output."""
        self.log_output.clear()
    
    def _open_output_folder(self):
        """Open the output folder in file explorer."""
        output_dir = self.output_dir_edit.text()
        if os.path.exists(output_dir):
            os.startfile(output_dir)
        else:
            QMessageBox.warning(
                self,
                "Folder Not Found",
                f"Output folder does not exist yet: {output_dir}\n\n"
                "Generate data first to create the folder."
            )
    
    # =========================================================================
    # FABRIC HANDLERS
    # =========================================================================
    
    def _browse_fabric_source_dir(self):
        """Browse for Fabric data source directory."""
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Data Source Directory", self.fabric_source_dir.text()
        )
        if dir_path:
            self.fabric_source_dir.setText(dir_path)
    
    def _fabric_log(self, message: str):
        """Add message to Fabric log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.fabric_log.append(f"[{timestamp}] {message}")
        self.fabric_log.moveCursor(QTextCursor.MoveOperation.End)
    
    def _refresh_capacities(self):
        """Fetch available Fabric capacities and populate dropdown."""
        if not self._fabric_credential:
            self._fabric_log("Please test connection first before loading capacities.")
            return
        
        self._fabric_log("Fetching available capacities...")
        self.fabric_refresh_cap_btn.setEnabled(False)
        self.fabric_capacity_info.setText("Loading capacities...")
        
        self._cap_worker = FabricCapacityWorker(credential=self._fabric_credential)
        self._cap_worker.result.connect(self._on_capacities_loaded)
        self._cap_worker.start()
    
    def _on_capacities_loaded(self, success: bool, data):
        """Handle capacity list result."""
        self.fabric_refresh_cap_btn.setEnabled(True)
        if success and isinstance(data, list):
            self.fabric_capacity_combo.clear()
            self.fabric_capacity_combo.addItem("-- Select a capacity --", "")
            for cap in data:
                display = f"{cap.get('displayName', 'N/A')} ({cap.get('sku', '')} - {cap.get('state', '')})"
                self.fabric_capacity_combo.addItem(display, cap.get("id", ""))
            self.fabric_capacity_info.setText(f"{len(data)} capacity(ies) found")
            self._fabric_log(f"Found {len(data)} capacities.")
        else:
            self.fabric_capacity_info.setText("Failed to load capacities")
            self._fabric_log(f"Failed to load capacities: {data}")
    
    def _test_fabric_connection(self):
        """Test Fabric connection by authenticating."""
        self._fabric_log("Testing connection...")
        self.fabric_status_label.setText("🔄 Authenticating...")
        self.fabric_status_label.setStyleSheet(f"color: {COLORS['warning']}; font-size: 12px;")
        
        # Run in background
        self._fabric_test_worker = FabricTestWorker(
            "browser" if self.fabric_auth_combo.currentIndex() == 0 else "device_code"
        )
        self._fabric_test_worker.result.connect(self._on_fabric_test_result)
        self._fabric_test_worker.start()
    
    def _on_fabric_test_result(self, success: bool, message: str, credential):
        """Handle Fabric connection test result."""
        if success:
            self._fabric_credential = credential  # Store for reuse
            self.fabric_status_label.setText("✅ Connected")
            self.fabric_status_label.setStyleSheet(f"color: {COLORS['success']}; font-size: 12px;")
            # Auto-load capacities after successful connection
            self._refresh_capacities()
        else:
            self._fabric_credential = None
            self.fabric_status_label.setText("❌ Failed")
            self.fabric_status_label.setStyleSheet(f"color: {COLORS['error']}; font-size: 12px;")
        self._fabric_log(message)
    
    def _start_fabric_upload(self):
        """Start uploading data to Fabric."""
        workspace_name = self.fabric_workspace_edit.text().strip()
        lakehouse_name = self.fabric_lakehouse_edit.text().strip()
        source_dir = self.fabric_source_dir.text().strip()
        
        if not workspace_name:
            QMessageBox.warning(self, "Missing Input", "Please enter a workspace name.")
            return
        if not lakehouse_name:
            QMessageBox.warning(self, "Missing Input", "Please enter a lakehouse name.")
            return
        if not os.path.isdir(source_dir):
            QMessageBox.warning(
                self, "Invalid Directory",
                f"Source directory does not exist: {source_dir}\n\n"
                "Generate data first, then upload to Fabric."
            )
            return
        
        # Get capacity
        capacity_id = self.fabric_capacity_combo.currentData() or ""
        
        # Check if authenticated
        if not self._fabric_credential:
            QMessageBox.warning(
                self, "Not Authenticated",
                "Please test Fabric connection first before uploading."
            )
            return
        
        # Determine file extension from format
        fmt = self.format_combo.currentText().lower() if hasattr(self, 'format_combo') else "csv"
        ext_map = {"csv": ".csv", "json": ".json", "parquet": ".parquet"}
        file_ext = ext_map.get(fmt, ".csv")
        
        # Update UI
        self.fabric_upload_btn.setEnabled(False)
        self.fabric_stop_btn.setEnabled(True)
        self.fabric_progress.setValue(0)
        
        self._fabric_log("=" * 50)
        self._fabric_log(f"Workspace: {workspace_name}")
        self._fabric_log(f"Lakehouse: {lakehouse_name}")
        if capacity_id:
            self._fabric_log(f"Capacity: {self.fabric_capacity_combo.currentText()}")
        self._fabric_log(f"Source: {source_dir} ({file_ext})")
        self._fabric_log("Files will be uploaded to OneLake")
        self._fabric_log("=" * 50)
        
        self.fabric_worker = FabricUploadWorker(
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            local_dir=source_dir,
            file_extension=file_ext,
            credential=self._fabric_credential,
            capacity_id=capacity_id,
        )
        self.fabric_worker.progress_updated.connect(self._on_fabric_progress)
        self.fabric_worker.upload_complete.connect(self._on_fabric_upload_complete)
        self.fabric_worker.upload_error.connect(self._on_fabric_upload_error)
        self.fabric_worker.start()
    
    def _stop_fabric_upload(self):
        """Stop Fabric upload."""
        if hasattr(self, 'fabric_worker') and self.fabric_worker:
            self.fabric_worker.terminate()
            self.fabric_worker = None
        self.fabric_upload_btn.setEnabled(True)
        self.fabric_stop_btn.setEnabled(False)
        self._fabric_log("Upload stopped by user.")
    
    def _on_fabric_progress(self, message: str, percentage: int):
        """Handle Fabric upload progress."""
        if percentage >= 0:
            self.fabric_progress.setValue(percentage)
        self._fabric_log(message)
    
    def _on_fabric_upload_complete(self, summary: dict):
        """Handle Fabric upload complete."""
        self.fabric_upload_btn.setEnabled(True)
        self.fabric_stop_btn.setEnabled(False)
        self.fabric_progress.setValue(100)
        
        failed_files = summary.get('failed_files', [])
        
        self._fabric_log("=" * 50)
        self._fabric_log("Upload to Fabric complete!")
        self._fabric_log(f"  Workspace: {summary['workspace']} ({summary['workspace_id']})")
        self._fabric_log(f"  Lakehouse: {summary['lakehouse']} ({summary['lakehouse_id']})")
        self._fabric_log(f"  Files uploaded: {summary['files_uploaded']}")
        if failed_files:
            self._fabric_log(f"  ⚠️ Failed files: {', '.join(failed_files)}")
        self._fabric_log("=" * 50)
        
        self.fabric_status_label.setText("✅ Connected")
        self.fabric_status_label.setStyleSheet(f"color: {COLORS['success']}; font-size: 12px;")
        
        # Build message
        msg = f"Successfully uploaded {summary['files_uploaded']} files to OneLake.\n\n"
        if failed_files:
            msg += f"⚠️ {len(failed_files)} file(s) failed: {', '.join(failed_files)}\n\n"
        msg += f"Workspace: {summary['workspace']}\n"
        msg += f"Lakehouse: {summary['lakehouse']}\n\n"
        msg += f"Files are in: Files/telco_data/"
        
        if failed_files:
            QMessageBox.warning(self, "Fabric Upload Complete (with failures)", msg)
        else:
            QMessageBox.information(self, "Fabric Upload Complete", msg)
    
    def _on_fabric_upload_error(self, error_message: str):
        """Handle Fabric upload error."""
        self.fabric_upload_btn.setEnabled(True)
        self.fabric_stop_btn.setEnabled(False)
        
        self._fabric_log(f"ERROR: {error_message}")
        
        QMessageBox.critical(self, "Fabric Upload Error", error_message)


class FabricTestWorker(QThread):
    """Quick background thread for testing Fabric authentication."""
    
    result = pyqtSignal(bool, str, object)  # success, message, credential
    
    def __init__(self, auth_method: str):
        super().__init__()
        self.auth_method = auth_method
    
    def run(self):
        try:
            client = FabricClient()
            ok = client.authenticate(method=self.auth_method)
            if ok:
                workspaces = client.list_workspaces()
                # Return credential for reuse
                self.result.emit(True, f"Connected. Found {len(workspaces)} workspace(s).", client.get_credential())
            else:
                self.result.emit(False, "Authentication failed.", None)
        except Exception as e:
            self.result.emit(False, f"Connection error: {e}", None)


class FabricCapacityWorker(QThread):
    """Background thread to fetch Fabric capacities."""
    
    result = pyqtSignal(bool, object)  # (success, list_of_capacities_or_error_str)
    
    def __init__(self, credential=None):
        super().__init__()
        self.credential = credential
    
    def run(self):
        try:
            client = FabricClient(credential=self.credential)
            # If no credential, this will fail - require test connection first
            if not client.is_authenticated():
                self.result.emit(False, "Not authenticated. Test connection first.")
                return
            capacities = client.list_capacities()
            self.result.emit(True, capacities)
        except Exception as e:
            self.result.emit(False, str(e))


def main():
    """Main entry point for the GUI application."""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = TelcoDataGeneratorGUI()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
