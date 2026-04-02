# ============================================================================
# Fabric Data Pipeline Orchestration
# ============================================================================
# Project: C360-Telco Medallion Architecture
# Engineer: Cooper (DevOps & Automation)
# Purpose: Create Bronze → Silver → Gold pipeline with scheduling & monitoring
# ============================================================================

param(
    [string]$WorkspaceName = "Telco",
    [string]$LakehouseName = "telco_data",
    [string]$PipelineName = "C360_Telco_Medallion_Pipeline"
)

$ErrorActionPreference = "Stop"

# ============================================================================
# Helper Functions
# ============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host "`n$('=' * 70)" -ForegroundColor Cyan
    Write-Host "⚙️  $Message" -ForegroundColor Yellow
    Write-Host "$('=' * 70)`n" -ForegroundColor Cyan
}

function Test-FabCli {
    try {
        $version = & fab --version 2>&1
        Write-Host "✓ fab CLI detected: $version" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "✗ fab CLI not found. Install from: https://github.com/microsoft/fabric-tooling" -ForegroundColor Red
        exit 1
    }
}

# ============================================================================
# Pipeline Definition
# ============================================================================

$pipelineDefinition = @{
    name = $PipelineName
    properties = @{
        description = "C360 Telco Medallion Architecture - Bronze → Silver → Gold"
        activities = @(
            # Activity 1: Bronze Ingestion
            @{
                name = "Bronze_Ingestion"
                type = "ExecuteNotebook"
                dependsOn = @()
                policy = @{
                    timeout = "02:00:00"
                    retry = 2
                    retryIntervalInSeconds = 300
                }
                userProperties = @()
                typeProperties = @{
                    notebookName = "01_bronze_ingestion"
                    parameters = @{
                        processing_date = @{
                            value = "@pipeline().parameters.processing_date"
                            type = "Expression"
                        }
                    }
                }
            },
            
            # Activity 2: Silver Transformations
            @{
                name = "Silver_Transformations"
                type = "ExecuteNotebook"
                dependsOn = @(
                    @{
                        activity = "Bronze_Ingestion"
                        dependencyConditions = @("Succeeded")
                    }
                )
                policy = @{
                    timeout = "03:00:00"
                    retry = 2
                    retryIntervalInSeconds = 300
                }
                userProperties = @()
                typeProperties = @{
                    notebookName = "02_silver_transformations"
                    parameters = @{
                        processing_date = @{
                            value = "@pipeline().parameters.processing_date"
                            type = "Expression"
                        }
                    }
                }
            },
            
            # Activity 3: Gold Aggregations
            @{
                name = "Gold_Aggregations"
                type = "ExecuteNotebook"
                dependsOn = @(
                    @{
                        activity = "Silver_Transformations"
                        dependencyConditions = @("Succeeded")
                    }
                )
                policy = @{
                    timeout = "03:00:00"
                    retry = 2
                    retryIntervalInSeconds = 300
                }
                userProperties = @()
                typeProperties = @{
                    notebookName = "03_gold_aggregations"
                    parameters = @{
                        processing_date = @{
                            value = "@pipeline().parameters.processing_date"
                            type = "Expression"
                        }
                    }
                }
            },
            
            # Activity 4: Data Quality Checks
            @{
                name = "Data_Quality_Checks"
                type = "ExecuteNotebook"
                dependsOn = @(
                    @{
                        activity = "Gold_Aggregations"
                        dependencyConditions = @("Succeeded")
                    }
                )
                policy = @{
                    timeout = "01:00:00"
                    retry = 1
                    retryIntervalInSeconds = 180
                }
                userProperties = @()
                typeProperties = @{
                    notebookName = "04_data_quality_checks"
                    parameters = @{
                        processing_date = @{
                            value = "@pipeline().parameters.processing_date"
                            type = "Expression"
                        }
                    }
                }
            },
            
            # Activity 5: Success Notification
            @{
                name = "Send_Success_Notification"
                type = "WebActivity"
                dependsOn = @(
                    @{
                        activity = "Data_Quality_Checks"
                        dependencyConditions = @("Succeeded")
                    }
                )
                policy = @{
                    timeout = "00:05:00"
                    retry = 0
                }
                userProperties = @()
                typeProperties = @{
                    url = "https://prod-00.westeurope.logic.azure.com:443/workflows/example/triggers/manual/paths/invoke"
                    method = "POST"
                    headers = @{
                        "Content-Type" = "application/json"
                    }
                    body = @{
                        pipeline = "@{pipeline().Pipeline}"
                        runId = "@{pipeline().RunId}"
                        status = "SUCCESS"
                        message = "C360 Telco medallion pipeline completed successfully"
                        timestamp = "@{utcnow()}"
                    } | ConvertTo-Json
                }
            }
        )
        parameters = @{
            processing_date = @{
                type = "String"
                defaultValue = "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            }
        }
        annotations = @(
            "medallion-architecture",
            "c360-telco",
            "daily-batch"
        )
    }
}

# ============================================================================
# Schedule Definition (Daily at 2 AM UTC)
# ============================================================================

$scheduleDefinition = @{
    name = "$PipelineName-Schedule"
    properties = @{
        description = "Daily execution at 2 AM UTC"
        pipelines = @(
            @{
                pipelineReference = @{
                    referenceName = $PipelineName
                    type = "PipelineReference"
                }
                parameters = @{}
            }
        )
        type = "ScheduleTrigger"
        typeProperties = @{
            recurrence = @{
                frequency = "Day"
                interval = 1
                startTime = "2024-01-01T02:00:00Z"
                timeZone = "UTC"
            }
        }
    }
}

# ============================================================================
# Main Execution
# ============================================================================

Write-Host "`n" + ("=" * 70)
Write-Host "⚙️  FABRIC PIPELINE ORCHESTRATION SETUP" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host "📅 Start Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor White
Write-Host "📁 Workspace: $WorkspaceName" -ForegroundColor White
Write-Host "🗂️  Pipeline: $PipelineName" -ForegroundColor White
Write-Host ("=" * 70) + "`n" -ForegroundColor Cyan

# Step 1: Validate fab CLI
Write-Step "Validating Prerequisites"
Test-FabCli

# Step 2: Get workspace ID
Write-Step "Retrieving Workspace Information"
$workspaces = & fab workspace list --output json | ConvertFrom-Json
$workspace = $workspaces | Where-Object { $_.displayName -eq $WorkspaceName }

if (-not $workspace) {
    Write-Host "✗ Workspace '$WorkspaceName' not found. Available workspaces:" -ForegroundColor Red
    $workspaces | ForEach-Object { Write-Host "  - $($_.displayName)" -ForegroundColor Yellow }
    exit 1
}

$workspaceId = $workspace.id
Write-Host "✓ Workspace ID: $workspaceId" -ForegroundColor Green

# Step 3: Save pipeline definition
Write-Step "Creating Pipeline Definition"
$pipelineFile = "$PSScriptRoot\pipeline-definition.json"
$pipelineDefinition | ConvertTo-Json -Depth 10 | Out-File -FilePath $pipelineFile -Encoding UTF8
Write-Host "✓ Pipeline definition saved to: $pipelineFile" -ForegroundColor Green

# Step 4: Create pipeline using fab CLI
Write-Step "Deploying Pipeline to Fabric"
try {
    # Note: Adjust fab CLI command based on actual syntax
    # This is a conceptual example - actual implementation depends on fab CLI version
    
    Write-Host "⚠️  Manual deployment required:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "1. Open Fabric workspace: $WorkspaceName" -ForegroundColor White
    Write-Host "2. Navigate to Data Factory section" -ForegroundColor White
    Write-Host "3. Create new Data Pipeline: $PipelineName" -ForegroundColor White
    Write-Host "4. Import definition from: $pipelineFile" -ForegroundColor White
    Write-Host ""
    Write-Host "📋 Pipeline Structure:" -ForegroundColor Cyan
    Write-Host "  └─ Bronze_Ingestion (01_bronze_ingestion.ipynb)" -ForegroundColor White
    Write-Host "      └─ Silver_Transformations (02_silver_transformations.ipynb)" -ForegroundColor White
    Write-Host "          └─ Gold_Aggregations (03_gold_aggregations.ipynb)" -ForegroundColor White
    Write-Host "              └─ Data_Quality_Checks (04_data_quality_checks.ipynb)" -ForegroundColor White
    Write-Host "                  └─ Send_Success_Notification" -ForegroundColor White
    Write-Host ""
    Write-Host "⏰ Schedule: Daily at 2:00 AM UTC" -ForegroundColor Cyan
    Write-Host ""
}
catch {
    Write-Host "✗ Pipeline creation failed: $_" -ForegroundColor Red
    exit 1
}

# Step 5: Save schedule definition
Write-Step "Creating Schedule Configuration"
$scheduleFile = "$PSScriptRoot\pipeline-schedule.json"
$scheduleDefinition | ConvertTo-Json -Depth 10 | Out-File -FilePath $scheduleFile -Encoding UTF8
Write-Host "✓ Schedule definition saved to: $scheduleFile" -ForegroundColor Green

# ============================================================================
# Summary
# ============================================================================

Write-Host "`n" + ("=" * 70)
Write-Host "✅ PIPELINE ORCHESTRATION SETUP COMPLETE" -ForegroundColor Green
Write-Host ("=" * 70) -ForegroundColor Cyan

Write-Host "`n📊 Deployment Summary:" -ForegroundColor Yellow
Write-Host "  ✓ Workspace: $WorkspaceName ($workspaceId)" -ForegroundColor White
Write-Host "  ✓ Pipeline Definition: $pipelineFile" -ForegroundColor White
Write-Host "  ✓ Schedule Definition: $scheduleFile" -ForegroundColor White

Write-Host "`n🔄 Pipeline Flow:" -ForegroundColor Yellow
Write-Host "  1️⃣  Bronze Ingestion (CSV → Delta, ~2 min)" -ForegroundColor White
Write-Host "  2️⃣  Silver Transformations (26 → 7 tables, ~3-5 min)" -ForegroundColor White
Write-Host "  3️⃣  Gold Aggregations (7 → 8 tables + ML, ~5-8 min)" -ForegroundColor White
Write-Host "  4️⃣  Data Quality Checks (~2 min)" -ForegroundColor White
Write-Host "  5️⃣  Success Notification (<1 min)" -ForegroundColor White
Write-Host "  📈 Total Runtime: ~15-20 minutes" -ForegroundColor Cyan

Write-Host "`n⏰ Execution Schedule:" -ForegroundColor Yellow
Write-Host "  • Frequency: Daily" -ForegroundColor White
Write-Host "  • Time: 2:00 AM UTC" -ForegroundColor White
Write-Host "  • Timezone: UTC" -ForegroundColor White
Write-Host "  • Start Date: 2024-01-01" -ForegroundColor White

Write-Host "`n🔔 Monitoring & Alerts:" -ForegroundColor Yellow
Write-Host "  • Pipeline failures trigger retry (up to 2 retries)" -ForegroundColor White
Write-Host "  • Success notification sent to Logic App webhook" -ForegroundColor White
Write-Host "  • Execution logs in Fabric monitoring" -ForegroundColor White

Write-Host "`n📚 Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Upload notebooks to Fabric workspace:" -ForegroundColor White
Write-Host "     - 01_bronze_ingestion.py" -ForegroundColor Gray
Write-Host "     - 02_silver_transformations.py" -ForegroundColor Gray
Write-Host "     - 03_gold_aggregations.py" -ForegroundColor Gray
Write-Host "  2. Import pipeline definition in Fabric UI" -ForegroundColor White
Write-Host "  3. Configure webhook URL for notifications" -ForegroundColor White
Write-Host "  4. Test pipeline with manual trigger" -ForegroundColor White
Write-Host "  5. Enable daily schedule" -ForegroundColor White

Write-Host "`n" + ("=" * 70) + "`n"
