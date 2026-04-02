"""
Telco Customer Data Generator - Flask Web Application

A web-based UI for generating synthetic Dutch Telco customer data
and uploading to Microsoft Fabric.
"""

import os
import json
import queue
import threading
import tempfile
import shutil
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

from config import GeneratorConfig, SMALL_CONFIG, MEDIUM_CONFIG, LARGE_CONFIG, ENTERPRISE_CONFIG
from generate import TelcoDataGenerator
from fabric_integration import FabricClient

# Build presets dictionary
CONFIG_PRESETS = {
    "SMALL": SMALL_CONFIG,
    "MEDIUM": MEDIUM_CONFIG,
    "LARGE": LARGE_CONFIG,
    "ENTERPRISE": ENTERPRISE_CONFIG,
}

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Global state for generation/upload progress
generation_status = {
    "running": False,
    "progress": 0,
    "message": "",
    "logs": [],
    "result": None,
    "error": None,
}

# Global Fabric state (persisted across requests)
fabric_client = None
fabric_authenticated = False
fabric_capacities = []

# Queue for real-time log streaming
log_queue = queue.Queue()


def reset_status():
    """Reset the global status."""
    global generation_status
    generation_status = {
        "running": False,
        "progress": 0,
        "message": "",
        "logs": [],
        "result": None,
        "error": None,
    }


def add_log(message: str, progress: int = -1):
    """Add a log message and optionally update progress."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    generation_status["logs"].append(log_entry)
    generation_status["message"] = message
    if progress >= 0:
        generation_status["progress"] = progress
    # Push to queue for SSE
    try:
        log_queue.put_nowait({"log": log_entry, "progress": generation_status["progress"]})
    except queue.Full:
        pass


@app.route("/")
def index():
    """Render the main page."""
    return render_template("index.html", presets=CONFIG_PRESETS)


@app.route("/api/presets")
def get_presets():
    """Get available presets."""
    presets = {}
    for name, config in CONFIG_PRESETS.items():
        presets[name] = {
            "num_parties": config.num_parties,
            "num_accounts": config.num_accounts,
            "num_subscribers": config.num_subscribers,
            "num_products": config.num_products,
            "subscriptions_per_subscriber": config.subscriptions_per_subscriber,
            "invoices_per_account": config.invoices_per_account,
            "prepaid_ratio": config.prepaid_ratio,
        }
    return jsonify(presets)


@app.route("/api/status")
def get_status():
    """Get current generation/upload status."""
    return jsonify(generation_status)


@app.route("/api/fabric/status")
def get_fabric_status():
    """Get Fabric authentication status."""
    global fabric_authenticated, fabric_capacities
    return jsonify({
        "authenticated": fabric_authenticated,
        "capacities": fabric_capacities,
    })


@app.route("/api/fabric/authenticate", methods=["POST"])
def fabric_authenticate():
    """Authenticate to Microsoft Fabric."""
    global fabric_client, fabric_authenticated
    
    data = request.json or {}
    auth_method = data.get("auth_method", "interactive")
    
    try:
        add_log(f"Authenticating to Fabric ({auth_method})...")
        
        fabric_client = FabricClient(log_callback=lambda msg: add_log(msg))
        ok = fabric_client.authenticate(method=auth_method)
        
        if ok:
            fabric_authenticated = True
            add_log("✅ Authentication successful!")
            return jsonify({"success": True, "message": "Authentication successful"})
        else:
            fabric_authenticated = False
            add_log("❌ Authentication failed")
            return jsonify({"success": False, "message": "Authentication failed"}), 401
    
    except Exception as e:
        fabric_authenticated = False
        error_msg = str(e)
        add_log(f"❌ Authentication error: {error_msg}")
        return jsonify({"success": False, "message": error_msg}), 500


@app.route("/api/fabric/capacities", methods=["GET"])
def get_capacities():
    """List available Fabric capacities."""
    global fabric_client, fabric_authenticated, fabric_capacities
    
    if not fabric_authenticated or not fabric_client:
        return jsonify({"error": "Not authenticated. Please authenticate first."}), 401
    
    try:
        add_log("Fetching Fabric capacities...")
        capacities = fabric_client.list_capacities()
        
        # Format for frontend
        fabric_capacities = [
            {
                "id": cap.get("id", ""),
                "displayName": cap.get("displayName", "Unknown"),
                "sku": cap.get("sku", ""),
                "region": cap.get("region", ""),
                "state": cap.get("state", ""),
            }
            for cap in capacities
        ]
        
        add_log(f"✅ Found {len(fabric_capacities)} capacities")
        return jsonify({"capacities": fabric_capacities})
    
    except Exception as e:
        error_msg = str(e)
        add_log(f"❌ Error listing capacities: {error_msg}")
        return jsonify({"error": error_msg}), 500


@app.route("/api/fabric/test", methods=["POST"])
def test_fabric_connection():
    """Test Fabric connection."""
    global fabric_client, fabric_authenticated, fabric_capacities
    
    data = request.json or {}
    auth_method = data.get("auth_method", "interactive")
    
    try:
        add_log("Testing Fabric connection...")
        
        # Create new client for test
        client = FabricClient(log_callback=lambda msg: add_log(msg))
        ok = client.authenticate(method=auth_method)
        
        if ok:
            # Try to list capacities as a real test
            capacities = client.list_capacities()
            fabric_client = client
            fabric_authenticated = True
            
            # Format capacities
            fabric_capacities = [
                {
                    "id": cap.get("id", ""),
                    "displayName": cap.get("displayName", "Unknown"),
                    "sku": cap.get("sku", ""),
                    "region": cap.get("region", ""),
                    "state": cap.get("state", ""),
                }
                for cap in capacities
            ]
            
            add_log(f"✅ Connection successful! Found {len(capacities)} capacities.")
            return jsonify({
                "success": True,
                "message": f"Connected successfully! Found {len(capacities)} capacities.",
                "capacities": fabric_capacities
            })
        else:
            add_log("❌ Connection failed")
            return jsonify({"success": False, "message": "Connection failed"}), 401
    
    except Exception as e:
        error_msg = str(e)
        add_log(f"❌ Connection test failed: {error_msg}")
        return jsonify({"success": False, "message": error_msg}), 500


@app.route("/api/stream")
def stream_logs():
    """Server-Sent Events endpoint for real-time log streaming."""
    def generate():
        while True:
            try:
                data = log_queue.get(timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
            except queue.Empty:
                # Send heartbeat
                yield f"data: {json.dumps({'heartbeat': True})}\n\n"
            except Exception:
                break
    
    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.route("/api/generate", methods=["POST"])
def generate_data():
    """Start data generation."""
    if generation_status["running"]:
        return jsonify({"error": "Generation already in progress"}), 400
    
    reset_status()
    generation_status["running"] = True
    
    data = request.json
    
    # Build config from request using ratio-based approach
    config = GeneratorConfig(
        num_parties=int(data.get("num_parties", 1000)),
        num_accounts=int(data.get("num_accounts", 800)),
        num_subscribers=int(data.get("num_subscribers", 1200)),
        num_products=int(data.get("num_products", 50)),
        subscriptions_per_subscriber=float(data.get("subscriptions_per_subscriber", 1.5)),
        invoices_per_account=float(data.get("invoices_per_account", 12.0)),
        prepaid_ratio=float(data.get("prepaid_ratio", 0.3)),
    )
    
    output_dir = data.get("output_dir", "")
    if not output_dir:
        output_dir = os.path.join(tempfile.gettempdir(), "telco_data")
    
    # Start generation in background thread
    thread = threading.Thread(
        target=_run_generation,
        args=(config, output_dir),
        daemon=True
    )
    thread.start()
    
    return jsonify({"status": "started", "output_dir": output_dir})


def _run_generation(config: GeneratorConfig, output_dir: str):
    """Background thread for data generation."""
    try:
        add_log(f"Starting data generation...", 5)
        add_log(f"Output directory: {output_dir}")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        generator = TelcoDataGenerator(config)
        
        # Generate all tables
        tables = [
            ("party", generator.generate_party),
            ("customer_account", generator.generate_customer_account),
            ("subscriber", generator.generate_subscriber),
            ("msisdn", generator.generate_msisdn),
            ("sim", generator.generate_sim),
            ("device", generator.generate_device),
            ("product_catalog", generator.generate_product_catalog),
            ("subscription", generator.generate_subscription),
            ("entitlement", generator.generate_entitlement),
            ("service", generator.generate_service),
            ("service_order", generator.generate_service_order),
            ("porting_request", generator.generate_porting_request),
            ("charge", generator.generate_charge),
            ("invoice", generator.generate_invoice),
            ("invoice_line", generator.generate_invoice_line),
            ("payment", generator.generate_payment),
            ("prepaid_balance_snapshot", generator.generate_prepaid_balance_snapshot),
            ("topup", generator.generate_topup),
            ("case_ticket", generator.generate_case_ticket),
            ("address", generator.generate_address),
            ("party_address", generator.generate_party_address),
            ("account_party_role", generator.generate_account_party_role),
            ("subscriber_status_history", generator.generate_subscriber_status_history),
            ("subscriber_msisdn", generator.generate_subscriber_msisdn),
            ("subscriber_sim", generator.generate_subscriber_sim),
            ("subscriber_device", generator.generate_subscriber_device),
        ]
        
        total = len(tables)
        for idx, (name, gen_func) in enumerate(tables, 1):
            progress = 5 + int(90 * idx / total)
            add_log(f"Generating {name}...", progress)
            
            df = gen_func()
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            add_log(f"  -> {name}.csv ({len(df)} rows)")
        
        add_log("Data generation complete!", 100)
        generation_status["running"] = False
        generation_status["result"] = {
            "success": True,
            "output_dir": output_dir,
            "tables_generated": total,
        }
        
    except Exception as e:
        add_log(f"ERROR: {str(e)}", -1)
        generation_status["running"] = False
        generation_status["error"] = str(e)


@app.route("/api/upload", methods=["POST"])
def upload_to_fabric():
    """Upload generated data to Microsoft Fabric."""
    global fabric_client, fabric_authenticated
    
    if generation_status["running"]:
        return jsonify({"error": "Operation already in progress"}), 400
    
    if not fabric_authenticated or not fabric_client:
        return jsonify({"error": "Not authenticated. Please test connection first."}), 401
    
    reset_status()
    generation_status["running"] = True
    
    data = request.json
    
    # Start upload in background thread
    thread = threading.Thread(
        target=_run_upload,
        args=(data, fabric_client),
        daemon=True
    )
    thread.start()
    
    return jsonify({"status": "started"})


def _run_upload(data: dict, client: FabricClient):
    """Background thread for Fabric upload."""
    try:
        workspace_name = data.get("workspace_name", "Teclo-E2E-Demo")
        lakehouse_name = data.get("lakehouse_name", "telco_data")
        capacity_id = data.get("capacity_id", "")
        local_dir = data.get("local_dir", "")
        
        if not local_dir or not os.path.isdir(local_dir):
            raise ValueError(f"Invalid data directory: {local_dir}")
        
        # Update client log callback (reusing authenticated session)
        client._log = lambda msg: add_log(msg)
        add_log("Using existing Fabric session...")
        
        add_log(f"Setting up workspace '{workspace_name}'...", 10)
        ws = client.get_or_create_workspace(
            workspace_name,
            capacity_id=capacity_id if capacity_id else None
        )
        workspace_id = ws["id"]
        add_log(f"Workspace ID: {workspace_id}")
        
        add_log(f"Setting up lakehouse '{lakehouse_name}'...", 20)
        lh = client.get_or_create_lakehouse(
            workspace_id, lakehouse_name, enable_schemas=True
        )
        lakehouse_id = lh["id"]
        add_log(f"Lakehouse ID: {lakehouse_id}")
        
        add_log("Uploading files to OneLake...", 30)
        
        def on_progress(filename, current, total):
            pct = 30 + int(65 * current / total)
            add_log(f"Uploading: {filename} ({current}/{total})", pct)
        
        # Upload files only (no table creation)
        result = client.upload_and_load_tables(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            local_dir=local_dir,
            remote_folder="Files/telco_data",
            file_extension=".csv",
            load_tables=False,  # Don't create tables
            progress_callback=on_progress,
        )
        
        failed_files = result.get("failed_files", [])
        if failed_files:
            add_log(f"⚠️ {len(failed_files)} file(s) failed: {', '.join(failed_files)}", 100)
        else:
            add_log("Upload complete!", 100)
        
        generation_status["running"] = False
        generation_status["result"] = {
            "success": True,
            "workspace": workspace_name,
            "workspace_id": workspace_id,
            "lakehouse": lakehouse_name,
            "lakehouse_id": lakehouse_id,
            "files_uploaded": result["files_uploaded"],
            "failed_files": failed_files,
        }
        
    except Exception as e:
        add_log(f"ERROR: {str(e)}", -1)
        generation_status["running"] = False
        generation_status["error"] = str(e)


@app.route("/api/stop", methods=["POST"])
def stop_operation():
    """Stop current operation (best effort)."""
    generation_status["running"] = False
    add_log("Stop requested...")
    return jsonify({"status": "stop_requested"})


if __name__ == "__main__":
    # Create templates directory if not exists
    templates_dir = os.path.join(os.path.dirname(__file__), "templates")
    os.makedirs(templates_dir, exist_ok=True)
    
    print("=" * 60)
    print("  Telco Data Generator - Web UI")
    print("=" * 60)
    print()
    print("  Open http://localhost:5000 in your browser")
    print()
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
