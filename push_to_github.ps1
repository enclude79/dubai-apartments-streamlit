[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
# PowerShell Script to add, commit, and push changes to GitHub

# Path to Git executable
$gitPath = "C:\Program Files\Git\cmd\git.exe"

# Prompt user for commit message
$commitMessage = Read-Host -Prompt "Enter commit message (e.g., 'Update data')"

if (-not $commitMessage) {
    $commitMessage = "Automatic commit: Update files"
    Write-Host "Using default commit message: $commitMessage"
}

# 1. Add all files
Write-Host "Adding all files to Git index..."
& $gitPath add .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git add .'"
    exit 1
}
Write-Host "Files added successfully."

# 2. Commit
Write-Host "Creating commit with message: '$commitMessage'..."
& $gitPath commit -m "$commitMessage"
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Error during 'git commit'. Maybe no changes to commit."
    # Do not exit, as it might be normal if there are no changes
}
else {
    Write-Host "Commit created successfully."
}

# 3. Push changes to remote repository
Write-Host "Pushing changes to GitHub remote repository..."
& $gitPath push
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git push'. Ensure your repository is configured correctly and you have access."
    exit 1
}
Write-Host "Changes pushed to GitHub successfully!"

Write-Host "Process completed."
# Keep window open to view results
# Read-Host -Prompt "Press Enter to exit" 