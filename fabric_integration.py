"""
Microsoft Fabric Integration Module

Handles connectivity to Microsoft Fabric for workspace, lakehouse creation,
and data upload via OneLake (ADLS Gen2 DFS endpoint).
"""

import os
import time
from typing import Optional, Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential


# Fabric REST API base
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS_BASE = "https://onelake.dfs.fabric.microsoft.com"

# Well-known Azure public client ID for PowerBI / Fabric
FABRIC_PUBLIC_CLIENT_ID = "ea0616ba-638b-4df5-95b9-636659ae5121"

# Scopes
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"


class FabricClient:
    """Client for Microsoft Fabric REST API and OneLake operations."""
    
    def __init__(self, log_callback: Optional[Callable[[str], None]] = None, credential=None):
        self._credential = credential
        self._token = None
        self._log = log_callback or print
    
    def is_authenticated(self) -> bool:
        """Check if client has valid credentials."""
        return self._credential is not None
    
    def get_credential(self):
        """Get the current credential for reuse in other clients."""
        return self._credential
    
    def set_credential(self, credential):
        """Set an existing credential (to avoid re-authentication)."""
        self._credential = credential
    
    def authenticate(self, method: str = "browser") -> bool:
        """Authenticate to Microsoft Fabric via Azure AD.
        
        Args:
            method: 'browser' for interactive browser login,
                    'device_code' for device code flow.
        Returns:
            True if authentication succeeded.
        """
        # If credential already set, validate it works
        if self._credential is not None:
            try:
                token = self._credential.get_token(FABRIC_API_SCOPE)
                self._token = token.token
                self._log("Using existing authentication session.")
                return True
            except Exception:
                self._log("Existing credential invalid, re-authenticating...")
                self._credential = None
        
        self._log("Authenticating to Microsoft Fabric...")
        
        try:
            if method == "browser":
                self._credential = InteractiveBrowserCredential(
                    client_id=FABRIC_PUBLIC_CLIENT_ID,
                )
            else:
                self._credential = DeviceCodeCredential(
                    client_id=FABRIC_PUBLIC_CLIENT_ID,
                )
            
            token = self._credential.get_token(FABRIC_API_SCOPE)
            self._token = token.token
            self._log("Authentication successful.")
            return True
        except Exception as e:
            self._log(f"Authentication failed: {e}")
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers for Fabric API."""
        if self._credential is None:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        
        token = self._credential.get_token(FABRIC_API_SCOPE)
        self._token = token.token
        
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
    
    def _get_onelake_headers(self) -> Dict[str, str]:
        """Get authorization headers for OneLake DFS API."""
        if self._credential is None:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        
        token = self._credential.get_token(ONELAKE_STORAGE_SCOPE)
        return {"Authorization": f"Bearer {token.token}"}
    
    def _handle_api_error(self, resp: requests.Response, context: str = ""):
        """Handle Fabric API errors with actionable messages."""
        if resp.status_code == 400:
            detail = ""
            try:
                error_json = resp.json()
                detail = error_json.get("error", {}).get("message", "")
                if not detail:
                    detail = error_json.get("message", resp.text)
            except Exception:
                detail = resp.text
            raise ValueError(f"400 Bad Request on {context}.\nServer response: {detail}")
        elif resp.status_code == 403:
            detail = ""
            try:
                detail = resp.json().get("error", {}).get("message", resp.text)
            except Exception:
                detail = resp.text
            raise PermissionError(
                f"403 Forbidden on {context}.\n{detail}\n\n"
                "Check: workspace role (Admin/Member/Contributor), tenant settings, capacity assignment."
            )
        elif resp.status_code == 401:
            raise PermissionError("401 Unauthorized: Token expired. Please re-authenticate.")
        resp.raise_for_status()

    # =========================================================================
    # CAPACITY OPERATIONS
    # =========================================================================

    def list_capacities(self) -> List[Dict]:
        """List all Fabric capacities accessible to the user."""
        headers = self._get_headers()
        resp = requests.get(f"{FABRIC_API_BASE}/capacities", headers=headers, timeout=30)
        self._handle_api_error(resp, "list capacities")
        return resp.json().get("value", [])

    def assign_capacity_to_workspace(self, workspace_id: str, capacity_id: str) -> None:
        """Assign a Fabric capacity to a workspace."""
        headers = self._get_headers()
        body = {"capacityId": capacity_id}
        self._log(f"Assigning capacity to workspace...")
        resp = requests.post(
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/assignToCapacity",
            headers=headers, json=body, timeout=30,
        )
        if resp.status_code == 202:
            time.sleep(5)
            return
        self._handle_api_error(resp, "assign capacity")

    # =========================================================================
    # WORKSPACE OPERATIONS
    # =========================================================================
    
    def list_workspaces(self) -> List[Dict]:
        """List all accessible Fabric workspaces."""
        headers = self._get_headers()
        resp = requests.get(f"{FABRIC_API_BASE}/workspaces", headers=headers, timeout=30)
        self._handle_api_error(resp, "list workspaces")
        return resp.json().get("value", [])
    
    def find_workspace(self, name: str) -> Optional[Dict]:
        """Find a workspace by name."""
        for ws in self.list_workspaces():
            if ws.get("displayName", "").lower() == name.lower():
                return ws
        return None
    
    def create_workspace(self, name: str, capacity_id: Optional[str] = None) -> Dict:
        """Create a new Fabric workspace."""
        headers = self._get_headers()
        body = {"displayName": name}
        if capacity_id:
            body["capacityId"] = capacity_id
        
        self._log(f"Creating workspace '{name}'...")
        resp = requests.post(f"{FABRIC_API_BASE}/workspaces", headers=headers, json=body, timeout=30)
        self._handle_api_error(resp, "create workspace")
        ws = resp.json()
        self._log(f"Workspace created (ID: {ws.get('id')}).")
        time.sleep(5)  # Wait for propagation
        return ws
    
    def get_or_create_workspace(self, name: str, capacity_id: Optional[str] = None) -> Dict:
        """Get existing workspace or create a new one."""
        ws = self.find_workspace(name)
        if ws:
            self._log(f"Workspace '{name}' exists (ID: {ws['id']}).")
            if capacity_id and ws.get("capacityId") != capacity_id:
                self.assign_capacity_to_workspace(ws["id"], capacity_id)
            return ws
        return self.create_workspace(name, capacity_id=capacity_id)

    # =========================================================================
    # LAKEHOUSE OPERATIONS
    # =========================================================================
    
    def list_items(self, workspace_id: str, item_type: str = "Lakehouse") -> List[Dict]:
        """List items in a workspace by type."""
        headers = self._get_headers()
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type={item_type}"
        
        for attempt in range(2):
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 403 and attempt == 0:
                self._log("Permission propagation delay, retrying...")
                time.sleep(10)
                headers = self._get_headers()
                continue
            break
        
        self._handle_api_error(resp, f"list items")
        return resp.json().get("value", [])
    
    def find_lakehouse(self, workspace_id: str, name: str) -> Optional[Dict]:
        """Find a lakehouse by name."""
        for item in self.list_items(workspace_id, "Lakehouse"):
            if item.get("displayName", "").lower() == name.lower():
                return item
        return None
    
    def create_lakehouse(self, workspace_id: str, name: str, enable_schemas: bool = False) -> Dict:
        """Create a new Lakehouse."""
        headers = self._get_headers()
        body = {"displayName": name, "type": "Lakehouse"}
        if enable_schemas:
            body["creationPayload"] = {"enableSchemas": True}
        
        self._log(f"Creating Lakehouse '{name}'...")
        resp = requests.post(
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items",
            headers=headers, json=body, timeout=60,
        )
        
        if resp.status_code not in (200, 201, 202):
            self._handle_api_error(resp, "create lakehouse")
        
        # Handle async creation
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            self._log("Lakehouse creation in progress...")
            
            for _ in range(30):
                time.sleep(retry_after)
                poll = requests.get(location, headers=self._get_headers(), timeout=30)
                if poll.status_code == 200:
                    result = poll.json()
                    status = result.get("status", "").lower()
                    if status in ("succeeded", "completed"):
                        lh = self.find_lakehouse(workspace_id, name)
                        return lh or result
                    elif status in ("failed", "cancelled"):
                        raise RuntimeError(f"Lakehouse creation failed: {result}")
            raise RuntimeError("Lakehouse creation timed out.")
        
        lh = resp.json()
        self._log(f"Lakehouse created (ID: {lh.get('id')}).")
        return lh
    
    def get_or_create_lakehouse(self, workspace_id: str, name: str, enable_schemas: bool = False) -> Dict:
        """Get existing lakehouse or create a new one."""
        lh = self.find_lakehouse(workspace_id, name)
        if lh:
            self._log(f"Lakehouse '{name}' exists (ID: {lh['id']}).")
            return lh
        return self.create_lakehouse(workspace_id, name, enable_schemas=enable_schemas)

    # =========================================================================
    # FILE UPLOAD OPERATIONS
    # =========================================================================
    
    def upload_file(self, workspace_id: str, lakehouse_id: str,
                    local_path: str, remote_folder: str = "Files", max_retries: int = 3) -> bool:
        """Upload a single file to OneLake with retry logic."""
        filename = os.path.basename(local_path)
        dfs_path = f"/{workspace_id}/{lakehouse_id}/{remote_folder}/{filename}"
        
        for attempt in range(max_retries):
            try:
                headers = self._get_onelake_headers()
                
                # Create file
                resp = requests.put(
                    f"{ONELAKE_DFS_BASE}{dfs_path}?resource=file",
                    headers=headers, timeout=60
                )
                resp.raise_for_status()

                # Upload content
                with open(local_path, "rb") as f:
                    data = f.read()

                resp = requests.patch(
                    f"{ONELAKE_DFS_BASE}{dfs_path}?action=append&position=0",
                    headers={**headers, "Content-Type": "application/octet-stream"},
                    data=data, timeout=180,
                )
                resp.raise_for_status()

                # Flush
                resp = requests.patch(
                    f"{ONELAKE_DFS_BASE}{dfs_path}?action=flush&position={len(data)}",
                    headers=headers, timeout=60
                )
                resp.raise_for_status()
                return True
                
            except (requests.exceptions.SSLError, 
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                    time.sleep(wait)
                else:
                    raise RuntimeError(f"Upload failed after {max_retries} retries: {e}")
            except requests.exceptions.HTTPError as e:
                # Don't retry HTTP errors (4xx, 5xx)
                raise RuntimeError(f"HTTP error: {e}")
        
        return False
    
    def upload_files(self, workspace_id: str, lakehouse_id: str, local_dir: str,
                     remote_folder: str = "Files/telco_data", file_extension: str = ".csv",
                     progress_callback: Optional[Callable[[str, int, int], None]] = None) -> Dict:
        """Upload all matching files from a directory to OneLake with retry for failures.
        
        Returns:
            Dict with 'files_uploaded' count.
        """
        # Create remote folder
        headers = self._get_onelake_headers()
        mkdir_url = f"{ONELAKE_DFS_BASE}/{workspace_id}/{lakehouse_id}/{remote_folder}?resource=directory"
        try:
            requests.put(mkdir_url, headers=headers, timeout=30)
        except Exception:
            pass
        
        # Find files
        files = sorted([
            f for f in os.listdir(local_dir)
            if f.endswith(file_extension) and os.path.isfile(os.path.join(local_dir, f))
        ])
        
        if not files:
            self._log(f"No {file_extension} files found in {local_dir}")
            return {"files_uploaded": 0}
        
        total = len(files)
        uploaded = 0
        failed_files = []
        self._log(f"Uploading {total} files...")
        
        def _upload_one(filename: str) -> tuple:
            try:
                self.upload_file(workspace_id, lakehouse_id, 
                                os.path.join(local_dir, filename), remote_folder)
                return (filename, True, None)
            except Exception as e:
                return (filename, False, str(e))
        
        # First pass - parallel upload with reduced concurrency to avoid overload
        max_workers = min(4, total)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_upload_one, f): f for f in files}
            done = 0
            for future in as_completed(futures):
                fname = futures[future]
                done += 1
                _, success, err = future.result()
                if progress_callback:
                    progress_callback(fname, done, total)
                if success:
                    uploaded += 1
                    self._log(f"  ✓ {fname}")
                else:
                    failed_files.append(fname)
                    self._log(f"  ✗ {fname}: {err}")
        
        # Retry failed files sequentially with longer delays
        if failed_files:
            self._log(f"Retrying {len(failed_files)} failed file(s)...")
            time.sleep(2)  # Brief pause before retry
            
            for fname in failed_files[:]:  # Iterate over copy
                try:
                    self._log(f"  Retrying: {fname}")
                    self.upload_file(workspace_id, lakehouse_id,
                                    os.path.join(local_dir, fname), remote_folder)
                    uploaded += 1
                    failed_files.remove(fname)
                    self._log(f"  ✓ {fname} (retry succeeded)")
                except Exception as e:
                    self._log(f"  ✗ {fname}: {e} (retry failed)")
                time.sleep(1)  # Delay between retries
        
        if failed_files:
            self._log(f"⚠️ {len(failed_files)} file(s) failed after retry: {', '.join(failed_files)}")
        
        self._log(f"Upload complete: {uploaded}/{total} files.")
        return {"files_uploaded": uploaded, "failed_files": failed_files}
    
    # Alias for backward compatibility
    def upload_and_load_tables(self, workspace_id: str, lakehouse_id: str, local_dir: str,
                               remote_folder: str = "Files/telco_data", file_extension: str = ".csv",
                               schema_name: str = None, load_tables: bool = False,
                               progress_callback: Optional[Callable[[str, int, int], None]] = None) -> Dict:
        """Upload files to OneLake. Table creation is not supported via API."""
        result = self.upload_files(workspace_id, lakehouse_id, local_dir, 
                                   remote_folder, file_extension, progress_callback)
        result["tables_created"] = 0
        return result
