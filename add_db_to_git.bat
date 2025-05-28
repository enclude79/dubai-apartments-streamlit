@echo off
cd /d %~dp0
set PATH=%PATH%;C:\Program Files\Git\bin

git add -f dubai_properties.db
git commit -m "Добавление файла базы данных SQLite для Streamlit Cloud"
git push

echo Файл базы данных успешно добавлен в репозиторий и отправлен на GitHub.
pause 