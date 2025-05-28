@echo off
chcp 65001
setlocal

echo Установлена кодировка UTF-8 (65001) для текущей сессии CMD.
echo.
echo Теперь для деплоя в Streamlit Cloud выполните следующие действия:
echo 1. Откройте страницу https://share.streamlit.io/deploy
echo 2. В поле "Repository" введите: enclude79/dubai-apartments-streamlit
echo 3. В поле "Main file path" введите: streamlit_sqlite_app.py
echo 4. Нажмите кнопку "Deploy"
echo.
echo Для исправления кодировки в будущих сессиях PowerShell выполните:
echo Set-ItemProperty HKCU:\Console CodePage -Value 65001
echo.
pause 