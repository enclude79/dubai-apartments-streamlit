@echo off
chcp 65001
setlocal enabledelayedexpansion

echo Проверка файлов в репозитории...
echo.

set REPO_URL=https://api.github.com/repos/enclude79/dubai-apartments-streamlit/contents

echo Получение списка файлов из репозитория...
curl -s %REPO_URL% > repo_files.json

echo.
echo Проверка наличия необходимых файлов:
echo.

set REQUIRED_FILES=streamlit_sqlite_app.py dubai_properties.db requirements.txt
set ALL_FOUND=true

for %%f in (%REQUIRED_FILES%) do (
    findstr /C:"\"name\":\"%%f\"" repo_files.json > nul
    if !errorlevel! equ 0 (
        echo [✓] %%f найден
    ) else (
        echo [X] %%f НЕ НАЙДЕН
        set ALL_FOUND=false
    )
)

echo.
if "%ALL_FOUND%"=="true" (
    echo Все необходимые файлы найдены в репозитории.
    echo.
    echo Действия для деплоя:
    echo 1. Откройте https://share.streamlit.io/deploy
    echo 2. Введите репозиторий: enclude79/dubai-apartments-streamlit
    echo 3. Введите файл: streamlit_sqlite_app.py
    echo 4. Нажмите "Deploy"
) else (
    echo ВНИМАНИЕ! Некоторые необходимые файлы отсутствуют в репозитории.
    echo Добавьте недостающие файлы перед деплоем.
)

del repo_files.json

echo.
pause 