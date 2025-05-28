@echo off
chcp 65001
cd /d %~dp0
setlocal enabledelayedexpansion

echo ======================================================
echo ПОЛНЫЙ ПРОЦЕСС ДЕПЛОЯ: GITHUB + STREAMLIT CLOUD
echo ======================================================
echo.

REM Проверяем наличие git
where git >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    set GIT_PATH=C:\Program Files\Git\bin\git.exe
) else (
    set GIT_PATH=git
)

echo ЭТАП 1: ЗАГРУЗКА ФАЙЛОВ В GITHUB
echo ------------------------------------------------------
echo.

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
"%GIT_PATH%" add README_SQLITE.md 2>nul
"%GIT_PATH%" add INSTRUCTIONS.md 2>nul

echo.
echo [3/4] Коммит и отправка файлов в GitHub...
"%GIT_PATH%" commit -m "Добавление файлов для деплоя в Streamlit Cloud"
"%GIT_PATH%" push origin master

echo.
echo [4/4] ЗАГРУЗКА В GITHUB ЗАВЕРШЕНА УСПЕШНО!
echo.
echo ------------------------------------------------------
echo.
echo ЭТАП 2: ДЕПЛОЙ В STREAMLIT CLOUD
echo ------------------------------------------------------
echo.

echo Для завершения деплоя вам нужно:
echo.
echo 1. Открыть браузер и перейти по адресу:
echo    https://share.streamlit.io/deploy
echo.
echo 2. Ввести следующие данные:
echo    Репозиторий: enclude79/dubai-apartments-streamlit
echo    Основной файл: streamlit_sqlite_app.py
echo.
echo 3. Нажать кнопку "Deploy"
echo.
echo ======================================================
echo.
echo Открыть страницу деплоя в браузере? (д/н)
set /p OPEN_BROWSER=

if /i "%OPEN_BROWSER%"=="д" (
    start https://share.streamlit.io/deploy
    echo.
    echo Браузер открыт! Выполните следующие действия:
    echo 1. Введите: enclude79/dubai-apartments-streamlit
    echo 2. Укажите файл: streamlit_sqlite_app.py
    echo 3. Нажмите "Deploy"
)

echo.
echo После деплоя ваше приложение будет доступно по адресу:
echo https://enclude79-dubai-apartments-streamlit.streamlit.app/
echo.
goto :end

:error
echo.
echo Процесс прерван из-за ошибки.
exit /b 1

:end
echo.
echo ПРОЦЕСС ДЕПЛОЯ ЗАВЕРШЕН!
echo ======================================================
pause 