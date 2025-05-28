@echo off
echo Запуск PostgreSQL и скрипта поиска дешевых квартир
echo =================================================
echo.

echo 1. Активация виртуального окружения...
call .\venv\Scripts\activate.bat

echo 2. Запуск скрипта проверки и запуска PostgreSQL...
python check_and_start_postgres.py

echo 3. Запуск скрипта поиска дешевых квартир...
python find_cheapest_apartments_langchain.py

echo.
echo Готово! Результаты сохранены в директорию reports/
echo.
pause 