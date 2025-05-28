@echo off
echo Запуск PostgreSQL...
"C:\Users\Administrator\anaconda3\Library\bin\pg_ctl.exe" -D "C:\PostgreSQLData" start
echo.
echo База данных успешно запущена!
echo.
pause 