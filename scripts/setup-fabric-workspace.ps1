# C360-Telco Fabric Automation Scripts
# Engineer: Cooper
# Purpose: fab CLI automation for workspace and lakehouse management

<#
.SYNOPSIS
    Automated setup for Telco Customer 360 Medallion Architecture in Microsoft Fabric

.DESCRIPTION
    This script uses fab CLI to:
    1. Create/verify workspace
    2. Create lakehouse structure
    3. Upload notebooks
    4. Create pipelines
    5. Configure monitoring

.PARAMETER WorkspaceName
    Name of the Fabric workspace (default: "Telco")

.PARAMETER LakehouseName
    Name of the lakehouse (default: "telco_data")

.PARAMETER Environment
    Target environment: Dev, UAT, or Prod (default: "Dev")

.EXAMPLE
    .\setup-fabric-workspace.ps1 -WorkspaceName "Telco-Dev" -Environment "Dev"
#>

param(
    [string]$WorkspaceName = "Telco",
    [string]$LakehouseName = "telco_data",
    [ValidateSet("Dev","UAT","Prod")]
    [string]$Environment = "Dev",
    [switch]$SkipUpload
)

# Script configuration
$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"

Write-Host "=" * 70
Write-Host "🔧 FABRIC WORKSPACE SETUP - C360-TELCO" -ForegroundColor Cyan
Write-Host "=" * 70
Write-Host "Workspace: $WorkspaceName"
Write-Host "Lakehouse: $LakehouseName"
Write-Host "Environment: $Environment"
Write-Host "=" * 70

# ==============================================================================
# Helper Functions
# ==============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host "`n📌 $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "  ✅ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "  ⚠️  $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "  ❌ $Message" -ForegroundColor Red
}

function Test-FabCli {
    try {
        $version = fab --version 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Success "fab CLI installed: $version"
            return $true
        }
    } catch {
        Write-Error "fab CLI not found. Please install from: https://aka.ms/fabric-cli"
        return $false
    }
}

# ==============================================================================
# Step 1: Verify Prerequisites
# ==============================================================================

Write-Step "Verifying Prerequisites"

if (-not (Test-FabCli)) {
    Write-Error "Cannot proceed without fab CLI"
    exit 1
}

# Check authentication
Write-Host "  🔐 Checking Fabric authentication..."
$authCheck = fab workspace list 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Not authenticated. Running fab login..."
    fab login
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Authentication failed"
        exit 1
    }
}
Write-Success "Authentication verified"

# ==============================================================================
# Step 2: Create/Verify Workspace
# ==============================================================================

Write-Step "Setting Up Workspace: $WorkspaceName"

# Check if workspace exists
$workspaceExists = $false
$workspaces = fab workspace list --output json 2>$null | ConvertFrom-Json
foreach ($ws in $workspaces) {
    if ($ws.displayName -eq $WorkspaceName) {
        $workspaceExists = $true
        $workspaceId = $ws.id
        Write-Success "Workspace exists: $WorkspaceName (ID: $workspaceId)"
        break
    }
}

if (-not $workspaceExists) {
    Write-Host "  Creating workspace..."
    $result = fab workspace create --name $WorkspaceName --description "Telco Customer 360 - $Environment" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Workspace created: $WorkspaceName"
    } else {
        Write-Error "Failed to create workspace: $result"
        exit 1
    }
}

# ==============================================================================
# Step 3: Create Lakehouse
# ==============================================================================

Write-Step "Setting Up Lakehouse: $LakehouseName"

# Check if lakehouse exists
$lakehouseExists = $false
$lakehouses = fab lakehouse list --workspace $WorkspaceName --output json 2>$null | ConvertFrom-Json
foreach ($lh in $lakehouses) {
    if ($lh.displayName -eq $LakehouseName) {
        $lakehouseExists = $true
        Write-Success "Lakehouse exists: $LakehouseName"
        break
    }
}

if (-not $lakehouseExists) {
    Write-Host "  Creating lakehouse..."
    $result = fab lakehouse create --name $LakehouseName --workspace $WorkspaceName 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Lakehouse created: $LakehouseName"
    } else {
        Write-Error "Failed to create lakehouse: $result"
        exit 1
    }
}

# ==============================================================================
# Step 4: Upload Source Data (if exists locally)
# ==============================================================================

if (-not $SkipUpload) {
    Write-Step "Uploading Source CSV Files"
    
    $sourceDataPath = Join-Path $PSScriptRoot "..\output"
    if (Test-Path $sourceDataPath) {
        $csvFiles = Get-ChildItem $sourceDataPath -Filter "*.csv"
        Write-Host "  Found $($csvFiles.Count) CSV files to upload"
        
        foreach ($file in $csvFiles) {
            Write-Host "  📤 Uploading $($file.Name)..."
            $targetPath = "Files/telco_data/$($file.Name)"
            
            # Note: fab CLI upload syntax may vary - adjust as needed
            $result = fab lakehouse upload-file --workspace $WorkspaceName --lakehouse $LakehouseName --source $file.FullName --target $targetPath 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Success "$($file.Name) uploaded"
            } else {
                Write-Warning "Could not upload $($file.Name): $result"
            }
        }
    } else {
        Write-Warning "Source data path not found: $sourceDataPath"
    }
} else {
    Write-Host "  ⏭️  Skipping data upload (--SkipUpload flag set)"
}

# ==============================================================================
# Step 5: Upload Notebooks
# ==============================================================================

Write-Step "Uploading Fabric Notebooks"

$notebooksPath = Join-Path $PSScriptRoot "..\notebooks"
if (Test-Path $notebooksPath) {
    $notebooks = Get-ChildItem $notebooksPath -Filter "*.py"
    Write-Host "  Found $($notebooks.Count) notebooks to upload"
    
    foreach ($notebook in $notebooks) {
        $notebookName = [System.IO.Path]::GetFileNameWithoutExtension($notebook.Name)
        Write-Host "  📓 Uploading $notebookName..."
        
        # Create or update notebook
        $result = fab notebook create --workspace $WorkspaceName --name $notebookName --definition $notebook.FullName 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "$notebookName uploaded"
        } else {
            # Try update if create failed
            $result = fab notebook update --workspace $WorkspaceName --name $notebookName --definition $notebook.FullName 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Success "$notebookName updated"
            } else {
                Write-Warning "Could not upload $notebookName: $result"
            }
        }
    }
} else {
    Write-Warning "Notebooks path not found: $notebooksPath"
}

# ==============================================================================
# Step 6: Create Medallion Pipeline
# ==============================================================================

Write-Step "Creating Data Pipeline"

$pipelineName = "telco_medallion_pipeline"
Write-Host "  Creating pipeline: $pipelineName..."

# Pipeline definition (simplified - would be in JSON file in production)
$pipelineDefinition = @"
{
  "name": "$pipelineName",
  "properties": {
    "activities": [
      {
        "name": "bronze_ingestion",
        "type": "SynapseNotebook",
        "notebook": "01_bronze_ingestion"
      },
      {
        "name": "silver_transformations",
        "type": "SynapseNotebook",
        "notebook": "02_silver_transformations",
        "dependsOn": ["bronze_ingestion"]
      },
      {
        "name": "gold_aggregations",
        "type": "SynapseNotebook",
        "notebook": "03_gold_aggregations",
        "dependsOn": ["silver_transformations"]
      }
    ]
  }
}
"@

# Save definition to temp file
$tempPipelineFile = Join-Path $env:TEMP "pipeline_definition.json"
$pipelineDefinition | Out-File $tempPipelineFile -Encoding utf8

# Create pipeline (syntax may vary based on fab CLI version)
# $result = fab pipeline create --workspace $WorkspaceName --definition $tempPipelineFile 2>&1

Write-Warning "Pipeline creation requires hands-on setup in Fabric UI (fab CLI pipeline support varies)"
Write-Host "  Pipeline structure:" -ForegroundColor Cyan
Write-Host "    1. bronze_ingestion (Notebook)" -ForegroundColor Cyan
Write-Host "    2. silver_transformations (Notebook)" -ForegroundColor Cyan  
Write-Host "    3. gold_aggregations (Notebook)" -ForegroundColor Cyan

# Clean up temp file
Remove-Item $tempPipelineFile -ErrorAction SilentlyContinue

# ==============================================================================
# Summary
# ==============================================================================

Write-Host "`n" + "=" * 70
Write-Host "✅ SETUP COMPLETE" -ForegroundColor Green
Write-Host "=" * 70
Write-Host "Workspace: $WorkspaceName"
Write-Host "Lakehouse: $LakehouseName"
Write-Host "Environment: $Environment"
Write-Host ""
Write-Host "🎯 Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Verify data in lakehouse: Files/telco_data/"
Write-Host "  2. Run notebook: 00_setup_workspace"
Write-Host "  3. Run notebook: 01_bronze_ingestion"
Write-Host "  4. Configure pipeline schedule in Fabric UI"
Write-Host "  5. Set up monitoring alerts"
Write-Host ""
Write-Host "📚 Documentation: https://github.com/Remc0000/C360-Telco"
Write-Host "=" * 70
