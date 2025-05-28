@echo off
chcp 65001
cd /d %~dp0
setlocal enabledelayedexpansion

echo ======================================================
echo АВТОМАТИЧЕСКИЙ ДЕПЛОЙ В STREAMLIT CLOUD
echo ======================================================
echo.

echo Для деплоя приложения в Streamlit Cloud вам нужно:
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
echo Автоматический деплой через API Streamlit невозможен, 
echo так как требуется авторизация через веб-интерфейс.
echo.
echo ======================================================
echo.
echo Хотите открыть страницу деплоя в браузере? (y/n)
set /p OPEN_BROWSER=

if /i "%OPEN_BROWSER%"=="y" (
    start https://share.streamlit.io/deploy
    echo.
    echo Браузер открыт! Выполните следующие действия:
    echo 1. Введите: enclude79/dubai-apartments-streamlit
    echo 2. Укажите файл: streamlit_sqlite_app.py
    echo 3. Нажмите "Deploy"
)

echo.
echo Ожидаю завершения деплоя...
echo После деплоя ваше приложение будет доступно по адресу:
echo https://enclude79-dubai-apartments-streamlit.streamlit.app/
echo.
pause 