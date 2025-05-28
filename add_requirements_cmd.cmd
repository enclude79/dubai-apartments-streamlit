@echo off
chcp 65001
cd /d %~dp0
set GIT_PATH=C:\Program Files\Git\bin\git.exe

echo Добавление файла requirements.txt в репозиторий...

"%GIT_PATH%" add requirements.txt
"%GIT_PATH%" commit -m "Добавление requirements.txt для Streamlit Cloud"
"%GIT_PATH%" push

echo.
echo Файл requirements.txt добавлен в репозиторий.
echo.
echo Теперь для деплоя в Streamlit Cloud:
echo 1. Откройте https://share.streamlit.io/deploy
echo 2. Репозиторий: enclude79/dubai-apartments-streamlit
echo 3. Файл: streamlit_sqlite_app.py
echo.
pause 