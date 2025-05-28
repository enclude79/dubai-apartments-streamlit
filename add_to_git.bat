@echo off
cd /d %~dp0
set PATH=%PATH%;C:\Program Files\Git\bin

git add streamlit_sqlite_app.py
git add dubai_properties.db
git add requirements.txt
git add INSTRUCTIONS.md
git add README_SQLITE.md
git add create_sample_sqlite_db.py
git add postgres_to_sqlite.py
git add schedule_export.py

git commit -m "Добавление SQLite-версии приложения для Streamlit Cloud"
git push

echo Файлы успешно добавлены в репозиторий и отправлены на GitHub.
pause 