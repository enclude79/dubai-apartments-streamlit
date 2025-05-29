[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$gitPath = "C:\Program Files\Git\cmd\git.exe"
$commitMessage = "Automatic commit: Update files"

Write-Host "Pulling latest changes from GitHub..."
& $gitPath pull --no-edit

Write-Host "Adding all files to Git index..."
& $gitPath add .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git add .'"
    exit 1
}
Write-Host "Files added successfully."

Write-Host "Creating commit with message: '$commitMessage'..."
& $gitPath commit -m "$commitMessage"
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Error during 'git commit'. Maybe no changes to commit."
}
else {
    Write-Host "Commit created successfully."
}

Write-Host "Pushing changes to GitHub remote repository..."
& $gitPath push
if ($LASTEXITCODE -ne 0) {
    Write-Error "Error during 'git push'. Ensure your repository is configured correctly and you have access."
    exit 1
}
Write-Host "Changes pushed to GitHub successfully!"
Write-Host "Process completed." 