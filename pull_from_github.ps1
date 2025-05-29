[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
# PowerShell Script to pull changes from GitHub

# Path to Git executable
$gitPath = "C:\Program Files\Git\cmd\git.exe"

# Pull changes
Write-Host "Pulling changes from GitHub..."
& $gitPath pull --no-edit
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git pull'. Ensure your repository is configured correctly."
    exit 1
}
Write-Host "Changes pulled from GitHub successfully!"

Write-Host "Process completed." 