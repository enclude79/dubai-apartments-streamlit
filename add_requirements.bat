@echo off
chcp 65001
cd /d %~dp0
set PATH=%PATH%;C:\Program Files\Git\bin

echo Добавление файла requirements.txt в репозиторий...

git add requirements.txt
git commit -m "Добавление requirements.txt для Streamlit Cloud"
git push

echo.
echo Файл requirements.txt добавлен в репозиторий.
echo.
echo Теперь для деплоя в Streamlit Cloud выполните следующие действия:
echo 1. Откройте страницу https://share.streamlit.io/deploy
echo 2. В поле "Repository" введите: enclude79/dubai-apartments-streamlit
echo 3. В поле "Main file path" введите: streamlit_sqlite_app.py
echo 4. Нажмите кнопку "Deploy"
echo.
pause 