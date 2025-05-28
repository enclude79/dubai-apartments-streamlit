@echo off
chcp 65001
cd /d %~dp0
setlocal enabledelayedexpansion

echo ======================================================
echo ЗАГРУЗКА ВСЕХ ФАЙЛОВ В GITHUB РЕПОЗИТОРИЙ
echo ======================================================
echo.

REM Проверяем наличие git
where git >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    set GIT_PATH=C:\Program Files\Git\bin\git.exe
) else (
    set GIT_PATH=git
)

echo [1/4] Проверка файлов для загрузки...
set FILES_TO_UPLOAD=streamlit_sqlite_app.py dubai_properties.db requirements.txt

for %%f in (%FILES_TO_UPLOAD%) do (
    if not exist "%%f" (
        echo ОШИБКА: Файл %%f не найден!
        echo Пожалуйста, убедитесь что этот файл существует.
        goto :error
    ) else (
        echo [OK] Файл %%f найден
    )
)

echo.
echo [2/4] Добавление файлов в репозиторий...

"%GIT_PATH%" add streamlit_sqlite_app.py
"%GIT_PATH%" add -f dubai_properties.db
"%GIT_PATH%" add requirements.txt
"%GIT_PATH%" add README_SQLITE.md
"%GIT_PATH%" add INSTRUCTIONS.md

echo.
echo [3/4] Коммит и отправка файлов в GitHub...
"%GIT_PATH%" commit -m "Добавление файлов для деплоя в Streamlit Cloud"
"%GIT_PATH%" push origin master

echo.
echo [4/4] Подготовка инструкций для деплоя...
echo.
echo ======================================================
echo ИНСТРУКЦИИ ПО ДЕПЛОЮ В STREAMLIT CLOUD
echo ======================================================
echo.
echo 1. Откройте страницу https://share.streamlit.io/deploy
echo 2. В поле "Repository" введите: enclude79/dubai-apartments-streamlit
echo 3. В поле "Main file path" введите: streamlit_sqlite_app.py
echo 4. Нажмите кнопку "Deploy"
echo.
echo После успешного деплоя приложение будет доступно по адресу:
echo https://enclude79-dubai-apartments-streamlit.streamlit.app/
echo.
echo ======================================================

goto :end

:error
echo.
echo Процесс прерван из-за ошибки.
exit /b 1

:end
echo.
echo Все файлы успешно загружены в GitHub.
echo Теперь можно выполнить деплой в Streamlit Cloud.
pause 