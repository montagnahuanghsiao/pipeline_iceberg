param(
    [string]$BronzeRoot = "C:\Users\yah51\Desktop\project\data\bronze",
    [ValidateSet("Move", "Copy")]
    [string]$Mode = "Move",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$products = @("CHL", "POC", "NFLH", "SST", "NSST", "SST4", "GFW")
$datePatterns = @(
    '(?<year>20\d{2})-(?<month>\d{2})-(?<day>\d{2})',
    '(?<year>20\d{2})(?<month>\d{2})(?<day>\d{2})'
)

function Get-DatePartsFromName {
    param([string]$Name)
    foreach ($pattern in $datePatterns) {
        $match = [regex]::Match($Name, $pattern)
        if ($match.Success) {
            return @{
                Year = $match.Groups["year"].Value
                Month = $match.Groups["month"].Value
                Day = $match.Groups["day"].Value
            }
        }
    }
    return $null
}

$root = Resolve-Path -LiteralPath $BronzeRoot
$moved = 0
$skipped = 0

foreach ($product in $products) {
    $productRoot = Join-Path $root $product
    if (-not (Test-Path -LiteralPath $productRoot -PathType Container)) {
        Write-Warning "Product directory not found: $productRoot"
        continue
    }

    Get-ChildItem -LiteralPath $productRoot -Recurse -File -Filter "*.parquet" |
        Where-Object {
            $_.FullName -notmatch '[\\/]+year=\d{4}[\\/]+month=\d{2}[\\/]'
        } |
        ForEach-Object {
            $dateParts = Get-DatePartsFromName -Name $_.Name
            if ($null -eq $dateParts) {
                Write-Warning "Cannot infer date from file name: $($_.FullName)"
                $script:skipped++
                return
            }

            $targetDir = Join-Path $productRoot ("year={0}\month={1}" -f $dateParts.Year, $dateParts.Month)
            $targetFile = Join-Path $targetDir $_.Name

            if ($_.FullName -eq $targetFile) {
                $script:skipped++
                return
            }

            if (Test-Path -LiteralPath $targetFile) {
                throw "Target already exists, refusing to overwrite: $targetFile"
            }

            Write-Host ("{0} {1} -> {2}" -f $Mode.ToUpperInvariant(), $_.FullName, $targetFile)
            if (-not $WhatIf) {
                New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
                if ($Mode -eq "Move") {
                    Move-Item -LiteralPath $_.FullName -Destination $targetFile
                } else {
                    Copy-Item -LiteralPath $_.FullName -Destination $targetFile
                }
            }
            $script:moved++
        }
}

Write-Host "BRONZE_LAYOUT status=complete mode=$Mode changed=$moved skipped=$skipped root=$root"
