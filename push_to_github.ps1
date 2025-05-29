[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
# PowerShell Script to add, commit, and push changes to GitHub

# Path to Git executable
$gitPath = "C:\Program Files\Git\cmd\git.exe"

# Add all changes
Write-Host "Adding changes to Git..."
& $gitPath add .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git add'"
    exit 1
}

# Commit changes
$commitMessage = "Update: Изменения в приложении Streamlit"
Write-Host "Committing changes..."
& $gitPath commit -m $commitMessage
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git commit'"
    exit 1
}

# Push changes
Write-Host "Pushing changes to GitHub..."
& $gitPath push
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git push'"
    exit 1
}

Write-Host "Changes pushed to GitHub successfully!"
Write-Host "Process completed."
# Keep window open to view results
# Read-Host -Prompt "Press Enter to exit" 