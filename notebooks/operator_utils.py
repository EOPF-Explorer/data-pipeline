"""Utilities for GeoZarr pipeline operator notebook.

This module provides helper functions for:
- Environment configuration
- kubectl detection and management
- RabbitMQ/AMQP operations
- Workflow monitoring
- STAC validation
"""

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pika


class Config:
    """Configuration manager for operator notebook."""

    def __init__(self, env_file: str | None = None):
        """Initialize configuration from environment variables.

        Args:
            env_file: Path to .env file (optional, defaults to .env in same directory)
        """
        # Load .env file if it exists
        if env_file is None:
            env_file = Path(__file__).parent / ".env"

        if Path(env_file).exists():
            self._load_env_file(env_file)

        # Kubernetes configuration
        self.kubeconfig = os.getenv(
            "KUBECONFIG",
            str(Path.home() / "Documents/Github/data-pipeline/.work/kubeconfig"),
        )
        self.namespace = os.getenv("NAMESPACE", "devseed")
        self.rabbitmq_namespace = os.getenv("RABBITMQ_NAMESPACE", "core")

        # RabbitMQ configuration
        self.rabbitmq_service = os.getenv("RABBITMQ_SERVICE", "rabbitmq")
        self.amqp_port = int(os.getenv("AMQP_PORT", "5672"))
        self.amqp_local_port = int(os.getenv("AMQP_LOCAL_PORT", "5672"))
        self.amqp_user = os.getenv("AMQP_USER", "user")
        self.amqp_password = os.getenv("AMQP_PASSWORD", "")

        # STAC endpoints
        self.stac_api = os.getenv("STAC_API", "https://api.explorer.eopf.copernicus.eu/stac")
        self.raster_api = os.getenv("RASTER_API", "https://api.explorer.eopf.copernicus.eu/raster")

        # Find kubectl
        self.kubectl = self._find_kubectl()

    def _load_env_file(self, env_file: str | Path) -> None:
        """Load environment variables from .env file."""
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, _, value = line.partition("=")
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")

    def _find_kubectl(self) -> str:
        """Find kubectl binary in common locations.

        Returns:
            Path to kubectl executable

        Raises:
            RuntimeError: If kubectl not found
        """
        locations = [
            "/opt/homebrew/bin/kubectl",  # Homebrew on Apple Silicon
            "/usr/local/bin/kubectl",  # Homebrew on Intel Mac / Docker Desktop
            "/usr/bin/kubectl",  # System installation
            "kubectl",  # In PATH
        ]

        print("🔍 Searching for kubectl...")
        for kubectl_path in locations:
            try:
                result = subprocess.run(
                    [kubectl_path, "version", "--client=true", "--output=yaml"],
                    capture_output=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    print(f"   ✅ Found: {kubectl_path}")
                    return kubectl_path
                else:
                    print(f"   ⚠️  Tried {kubectl_path}: exit code {result.returncode}")
            except FileNotFoundError:
                print(f"   ❌ Not found: {kubectl_path}")
            except subprocess.TimeoutExpired:
                print(f"   ⏱️  Timeout: {kubectl_path}")
            except Exception as e:
                print(f"   ❌ Error with {kubectl_path}: {e}")

        raise RuntimeError(
            "kubectl not found!\n"
            "Install with: brew install kubectl\n"
            "Or install Docker Desktop (includes kubectl)"
        )

    def verify(self) -> bool:
        """Verify configuration is valid.

        Returns:
            True if configuration is valid
        """
        print("\n🔧 Configuration:")
        print(f"  kubectl: {self.kubectl}")
        print(f"  Kubeconfig: {self.kubeconfig}")
        print(f"  Workflow Namespace: {self.namespace}")
        print(f"  RabbitMQ Namespace: {self.rabbitmq_namespace}")
        print(f"  RabbitMQ Service: {self.rabbitmq_service}")
        print(f"  AMQP User: {self.amqp_user}")
        print(f"  AMQP Password: {'***' if self.amqp_password else '(not set)'}")
        print(f"  STAC API: {self.stac_api}")
        print(f"  Raster API: {self.raster_api}")

        # Check kubeconfig exists
        if not Path(self.kubeconfig).exists():
            print(f"\n⚠️  Kubeconfig not found: {self.kubeconfig}")
            print("   Update KUBECONFIG in .env file")
            return False
        print("\n✅ Kubeconfig exists")

        # Check pika installed
        print("✅ pika library available")

        # Check RabbitMQ service
        print(f"\n🐰 Checking RabbitMQ service in {self.rabbitmq_namespace}...")
        check_result = subprocess.run(
            [
                self.kubectl,
                "get",
                "svc",
                self.rabbitmq_service,
                "-n",
                self.rabbitmq_namespace,
            ],
            env={"KUBECONFIG": self.kubeconfig},
            capture_output=True,
            text=True,
        )

        if check_result.returncode == 0:
            print(
                f"   ✅ RabbitMQ service found: {self.rabbitmq_service}.{self.rabbitmq_namespace}"
            )
        else:
            print(f"   ❌ RabbitMQ service not found in {self.rabbitmq_namespace} namespace")
            return False

        # Check password is set
        if not self.amqp_password:
            print("\n⚠️  AMQP_PASSWORD not set!")
            print("   Get password with:")
            print(
                f"   kubectl get secret rabbitmq-password -n {self.rabbitmq_namespace} "
                "-o jsonpath='{.data.rabbitmq-password}' | base64 -d"
            )
            return False

        return True


def start_port_forward(config: Config) -> subprocess.Popen:
    """Start port-forward to RabbitMQ service.

    Args:
        config: Configuration object

    Returns:
        Popen object for the port-forward process
    """
    print("\n🔌 Setting up RabbitMQ port-forward...")
    print("   (This will run in background - ignore if already forwarding)")

    cmd = [
        config.kubectl,
        "port-forward",
        f"svc/{config.rabbitmq_service}",
        f"{config.amqp_local_port}:{config.amqp_port}",
        "-n",
        config.rabbitmq_namespace,
    ]

    print(f"   Command: {' '.join(cmd)}")
    print("   (If this fails, the port may already be forwarded - that's OK)")

    try:
        proc = subprocess.Popen(
            cmd,
            env={"KUBECONFIG": config.kubeconfig},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(2)  # Give it a moment to start
        print("✅ Port-forward started")
        return proc
    except Exception as e:
        print(f"⚠️  Port-forward error (may already be running): {e}")
        return None


def publish_amqp_message(
    config: Config,
    payload: dict[str, Any],
    exchange: str = "geozarr",
    routing_key: str = "eopf.items.convert",
) -> str | None:
    """Publish message to RabbitMQ via AMQP.

    Args:
        config: Configuration object
        payload: Message payload (will be JSON-encoded)
        exchange: AMQP exchange name
        routing_key: AMQP routing key

    Returns:
        Item ID if successful, None otherwise
    """
    # Derive item_id if not in payload (Sensor expects it!)
    item_id = payload.get("item_id")
    if not item_id:
        # Extract from source_url: .../items/{item_id}
        item_id = payload["source_url"].rstrip("/").split("/")[-1]
        payload["item_id"] = item_id
        print(f"💡 Auto-derived item_id: {item_id}")

    print("📝 Payload:")
    print(json.dumps(payload, indent=2))

    print("\n🚀 Publishing to RabbitMQ...")

    try:
        # Connect to RabbitMQ
        credentials = pika.PlainCredentials(config.amqp_user, config.amqp_password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="localhost",
                port=config.amqp_local_port,
                credentials=credentials,
                virtual_host="/",
            )
        )
        channel = connection.channel()

        # Declare exchange
        channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)

        # Publish message
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="application/json",
            ),
        )

        connection.close()

        print("✅ Payload published successfully!")
        print(f"   Exchange: {exchange}")
        print(f"   Routing key: {routing_key}")
        print(f"   Output item ID: {item_id}")
        print(f"   Collection: {payload['collection']}")

        return item_id

    except pika.exceptions.AMQPConnectionError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("   1. Check port-forward is running:")
        print(
            f"      {config.kubectl} port-forward -n {config.rabbitmq_namespace} "
            f"svc/{config.rabbitmq_service} {config.amqp_local_port}:{config.amqp_port}"
        )
        print("   2. Verify AMQP credentials in .env file")
        print("      Default user: 'user'")
        print(
            f"      Get password: {config.kubectl} get secret rabbitmq-password "
            f"-n {config.rabbitmq_namespace} -o jsonpath='{{.data.rabbitmq-password}}' | base64 -d"
        )
        print("   3. Check RabbitMQ service is running:")
        print(
            f"      {config.kubectl} get svc {config.rabbitmq_service} -n {config.rabbitmq_namespace}"
        )
        print("   4. Check RabbitMQ pod status:")
        print(f"      {config.kubectl} get pods -n {config.rabbitmq_namespace} | grep rabbitmq")
        return None
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return None


def get_latest_workflow(config: Config, min_age_seconds: int = 0) -> str | None:
    """Get most recent workflow name.

    Args:
        config: Configuration object
        min_age_seconds: Only return workflows created within this many seconds (0 = any age)

    Returns:
        Workflow name if found, None otherwise
    """
    import subprocess
    from datetime import UTC, datetime

    try:
        result = subprocess.run(
            [
                config.kubectl,
                "get",
                "wf",
                "-n",
                config.namespace,
                "--sort-by=.metadata.creationTimestamp",
                "-o=jsonpath={.items[-1].metadata.name},{.items[-1].metadata.creationTimestamp}",
            ],
            capture_output=True,
            text=True,
            check=True,
            env={"KUBECONFIG": config.kubeconfig},
        )

        output = result.stdout.strip()
        if not output or "," not in output:
            return None

        name, timestamp = output.rsplit(",", 1)

        # Check age if min_age_seconds > 0
        if min_age_seconds > 0:
            # Parse ISO 8601 timestamp
            created = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            now = datetime.now(UTC)
            age = (now - created).total_seconds()

            if age > min_age_seconds:
                print(f"⚠️  Latest workflow is {int(age)}s old (expected < {min_age_seconds}s)")
                return None

        return name
    except Exception as e:
        print(f"⚠️  Could not get latest workflow: {e}")
        return None


def get_workflow_status(config: Config, workflow_name: str) -> dict[str, Any]:
    """Get workflow status and details.

    Args:
        config: Configuration object
        workflow_name: Name of the workflow

    Returns:
        Workflow status dict
    """
    result = subprocess.run(
        [config.kubectl, "get", "wf", workflow_name, "-n", config.namespace, "-o", "json"],
        env={"KUBECONFIG": config.kubeconfig},
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        return json.loads(result.stdout)
    return {}


def get_pod_logs(config: Config, workflow_name: str, step_name: str) -> str:
    """Get logs from workflow step pod.

    Args:
        config: Configuration object
        workflow_name: Name of the workflow
        step_name: Name of the step (convert, register, augment)

    Returns:
        Pod logs as string
    """
    # Find pod for this step
    pod_result = subprocess.run(
        [
            config.kubectl,
            "get",
            "pods",
            "-n",
            config.namespace,
            "-l",
            f"workflows.argoproj.io/workflow={workflow_name}",
            "-o",
            "json",
        ],
        env={"KUBECONFIG": config.kubeconfig},
        capture_output=True,
        text=True,
    )

    if pod_result.returncode != 0:
        return "No pods found"

    try:
        pods_data = json.loads(pod_result.stdout)
        pods = pods_data.get("items", [])

        # Find pod matching step name
        for pod in pods:
            pod_name = pod["metadata"]["name"]
            if step_name in pod_name:
                # Get logs
                log_result = subprocess.run(
                    [config.kubectl, "logs", pod_name, "-n", config.namespace, "--tail=100"],
                    env={"KUBECONFIG": config.kubeconfig},
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                return log_result.stdout if log_result.returncode == 0 else log_result.stderr

        return f"No pod found for step: {step_name}"
    except Exception as e:
        return f"Error getting logs: {e}"


def monitor_workflow(config: Config, workflow_name: str, timeout_minutes: int = 5) -> bool:
    """Monitor workflow execution until completion.

    Args:
        config: Configuration object
        workflow_name: Name of the workflow to monitor
        timeout_minutes: Maximum time to wait (default: 5 minutes)

    Returns:
        True if workflow succeeded, False otherwise
    """
    print(f"� Monitoring: {workflow_name}")
    print("=" * 60)

    max_iterations = (timeout_minutes * 60) // 5
    last_phase = None

    for i in range(max_iterations):
        wf_data = get_workflow_status(config, workflow_name)

        if not wf_data:
            print("❌ Workflow not found")
            return False

        phase = wf_data.get("status", {}).get("phase", "Unknown")
        progress = wf_data.get("status", {}).get("progress", "")

        # Only print when phase changes or every 30s
        if phase != last_phase or i % 6 == 0:
            elapsed = i * 5
            status_icon = (
                "🔄"
                if phase == "Running"
                else "⏳"
                if phase == "Pending"
                else "✅"
                if phase == "Succeeded"
                else "❌"
            )
            print(f"{status_icon} [{elapsed:3d}s] {phase:12s} {progress}")
            last_phase = phase

        if phase in ["Succeeded", "Failed", "Error"]:
            print("=" * 60)
            print(f"\n{'✅ SUCCESS' if phase == 'Succeeded' else '❌ FAILED'}: Workflow {phase}\n")

            # Show final logs for each step
            steps = ["convert", "register", "augment"]
            for step in steps:
                print(f"📄 {step.upper()} Logs (last 20 lines):")
                print("-" * 60)
                logs = get_pod_logs(config, workflow_name, step)
                # Show last 20 lines
                log_lines = logs.split("\n")
                print("\n".join(log_lines[-20:]))
                print()

            return phase == "Succeeded"

        time.sleep(5)

    print("=" * 60)
    print(f"\n⏱️  Timeout: Still running after {timeout_minutes} minutes")
    print(f"💡 Check status: {config.kubectl} get wf {workflow_name} -n {config.namespace} -w")
    return False


def validate_stac_item(config: Config, item_id: str, collection: str) -> bool:
    """Validate STAC item and check visualization links.

    Args:
        config: Configuration object
        item_id: STAC item ID
        collection: STAC collection ID

    Returns:
        True if validation successful, False otherwise
    """
    import requests

    stac_item_url = f"{config.stac_api}/collections/{collection}/items/{item_id}"

    print(f"🔍 Validating results for: {item_id}\n")

    # 1. Check STAC item exists
    print("1. Checking STAC item...")
    try:
        response = requests.get(stac_item_url, timeout=10)
        if response.status_code != 200:
            print(f"   ❌ STAC item not found: {response.status_code}")
            print(f"   URL: {stac_item_url}")
            return False

        stac_item = response.json()
        print("   ✅ STAC item found")

        # Check CRS
        proj_epsg = stac_item.get("properties", {}).get("proj:epsg")
        print(f"   📍 CRS: EPSG:{proj_epsg}")

        # Check assets
        assets = list(stac_item.get("assets", {}).keys())
        print(f"   📦 Assets: {len(assets)} found")
        if assets:
            print(f"      {', '.join(assets[:5])}" + ("..." if len(assets) > 5 else ""))

        # Check for GeoZarr asset
        geozarr_assets = [k for k in assets if "geozarr" in k.lower() or "r10m" in k.lower()]
        if geozarr_assets:
            print(f"   ✅ GeoZarr assets: {', '.join(geozarr_assets[:3])}")

        # Check links
        links = stac_item.get("links", [])
        viewer_link = next((link for link in links if link.get("rel") == "viewer"), None)
        xyz_link = next((link for link in links if link.get("rel") == "xyz"), None)
        tilejson_link = next((link for link in links if link.get("rel") == "tilejson"), None)

        print("   🔗 Visualization Links:")
        print(f"      Viewer: {'✅' if viewer_link else '❌'}")
        print(f"      XYZ: {'✅' if xyz_link else '❌'}")
        print(f"      TileJSON: {'✅' if tilejson_link else '❌'}")

        # 2. Test TiTiler
        print("\n2. Testing TiTiler access...")
        if assets and proj_epsg:
            titiler_info_url = f"{config.raster_api}/stac/info?url={stac_item_url}"
            try:
                info_response = requests.get(titiler_info_url, timeout=15)
                if info_response.status_code == 200:
                    print("   ✅ TiTiler accessible")
                    info_data = info_response.json()
                    bands = list(info_data.keys())
                    if bands:
                        print(f"   📊 Bands available: {len(bands)}")
                        print(f"      {', '.join(bands[:5])}" + ("..." if len(bands) > 5 else ""))
                else:
                    print(f"   ⚠️  TiTiler returned: {info_response.status_code}")
            except Exception as e:
                print(f"   ⚠️  TiTiler error: {e}")

        # 3. Display viewer link
        print("\n3. Map Viewer:")
        if viewer_link:
            print(f"   🗺️  {viewer_link['href']}")
            print("\n   👆 Open this URL to view the map!")
        else:
            print("   ❌ No viewer link found")

        print("\n✅ Validation complete!")
        return True

    except requests.exceptions.Timeout:
        print(f"   ⏱️  Request timeout: {stac_item_url}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def create_search_ui(payload_file: Path):
    """Create interactive STAC search UI.

    Args:
        payload_file: Path to payload.json file to update

    Returns:
        IPython display object
    """
    from datetime import UTC, datetime, timedelta

    import ipywidgets as W
    from ipyleaflet import DrawControl, Map, basemap_to_tiles, basemaps
    from IPython.display import display
    from pystac_client import Client

    # Create map
    m = Map(
        center=(48.0, 10.0),
        zoom=5,
        basemap=basemap_to_tiles(basemaps.OpenStreetMap.Mapnik),
        scroll_wheel_zoom=True,
    )

    # Drawing control
    draw_control = DrawControl(
        rectangle={"shapeOptions": {"color": "#3388ff"}},
        polygon={},
        polyline={},
        circle={},
        marker={},
        circlemarker={},
    )
    drawn_bbox = None

    def handle_draw(target, action, geo_json):
        nonlocal drawn_bbox
        if action == "created":
            coords = geo_json["geometry"]["coordinates"][0]
            lons, lats = [c[0] for c in coords], [c[1] for c in coords]
            drawn_bbox = [min(lons), min(lats), max(lons), max(lats)]
            bbox_input.value = (
                f"{drawn_bbox[0]:.4f},{drawn_bbox[1]:.4f},{drawn_bbox[2]:.4f},{drawn_bbox[3]:.4f}"
            )

    draw_control.on_draw(handle_draw)
    m.add_control(draw_control)

    # Search parameters
    collection_input = W.Dropdown(
        options=["sentinel-2-l2a"],
        value="sentinel-2-l2a",
        description="Collection:",
        style={"description_width": "120px"},
    )
    bbox_input = W.Text(
        value="12.3,41.8,12.5,41.9",
        description="BBox:",
        placeholder="minx,miny,maxx,maxy",
        style={"description_width": "120px"},
    )
    date_start = W.DatePicker(
        value=(datetime.now() - timedelta(days=30)).date(),
        description="Start:",
        style={"description_width": "120px"},
    )
    date_end = W.DatePicker(
        value=datetime.now().date(), description="End:", style={"description_width": "120px"}
    )
    max_cloud = W.IntSlider(
        value=20, min=0, max=100, description="Max cloud %:", style={"description_width": "120px"}
    )
    limit_input = W.IntSlider(
        value=5, min=1, max=20, description="Max results:", style={"description_width": "120px"}
    )

    # Results
    results_output = W.Output()
    search_results = []
    item_selector = W.Dropdown(
        options=[],
        description="Select:",
        style={"description_width": "120px"},
        layout=W.Layout(width="600px", visibility="hidden"),
    )
    update_btn = W.Button(
        description="📝 Update payload",
        button_style="success",
        layout=W.Layout(visibility="hidden"),
    )
    status_output = W.Output()

    def search_stac(b):
        nonlocal search_results
        with results_output:
            results_output.clear_output()
            print("🔍 Searching...")
            try:
                bbox = [float(x.strip()) for x in bbox_input.value.split(",")]
                dt_start = datetime.combine(date_start.value, datetime.min.time()).replace(
                    tzinfo=UTC
                )
                dt_end = datetime.combine(date_end.value, datetime.max.time()).replace(tzinfo=UTC)

                client = Client.open("https://stac.core.eopf.eodc.eu")
                search = client.search(
                    collections=[collection_input.value],
                    bbox=bbox,
                    datetime=f"{dt_start.isoformat()}/{dt_end.isoformat()}",
                    query={"eo:cloud_cover": {"lt": max_cloud.value}},
                    max_items=limit_input.value,
                )
                search_results = list(search.items())

                print(f"✅ Found {len(search_results)} items\n")
                if search_results:
                    item_options = []
                    for i, item in enumerate(search_results, 1):
                        cloud = item.properties.get("eo:cloud_cover", "?")
                        date = item.datetime.strftime("%Y-%m-%d") if item.datetime else "?"
                        label = f"{i}. {item.id} ({date}, {cloud}% cloud)"
                        item_options.append((label, i - 1))
                        print(f"{i}. {item.id} - {date}, {cloud}% cloud")

                    item_selector.options = item_options
                    item_selector.value = 0
                    item_selector.layout.visibility = "visible"
                    update_btn.layout.visibility = "visible"
                    print("\n💡 Select item and click 'Update payload'")
                else:
                    print("No items found. Adjust parameters.")
                    item_selector.layout.visibility = "hidden"
                    update_btn.layout.visibility = "hidden"
            except Exception as e:
                print(f"❌ Search failed: {e}")

    def update_payload(b):
        with status_output:
            status_output.clear_output()
            if not search_results:
                print("❌ No results")
                return
            try:
                selected_item = search_results[item_selector.value]
                with open(payload_file) as f:
                    current_payload = json.load(f)

                new_url = f"https://stac.core.eopf.eodc.eu/collections/{collection_input.value}/items/{selected_item.id}"
                current_payload["source_url"] = new_url

                with open(payload_file, "w") as f:
                    json.dump(current_payload, f, indent=4)

                print(f"✅ Updated! {selected_item.id}")
                print(
                    f"   {selected_item.datetime.strftime('%Y-%m-%d') if selected_item.datetime else '?'}"
                )
                print("\n💡 Re-run Cell 2 to reload")
            except Exception as e:
                print(f"❌ Failed: {e}")

    search_btn = W.Button(description="🔍 Search", button_style="primary")
    search_btn.on_click(search_stac)
    update_btn.on_click(update_payload)

    ui = W.VBox(
        [
            W.HTML("<h4>📍 Draw bbox or enter coordinates</h4>"),
            m,
            W.HTML("<h4>🔎 Configure search</h4>"),
            W.HBox([collection_input, bbox_input]),
            W.HBox([date_start, date_end]),
            W.HBox([max_cloud, limit_input]),
            search_btn,
            W.HTML("<h4>📊 Results</h4>"),
            results_output,
            item_selector,
            update_btn,
            status_output,
        ]
    )

    return display(ui)
