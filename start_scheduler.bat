@echo off
echo Запуск планировщика публикаций...
cd /d "%~dp0"
python publication_scheduler.py start
pause 