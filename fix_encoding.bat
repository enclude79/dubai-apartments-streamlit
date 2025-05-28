@echo off
cd /d %~dp0
set PATH=%PATH%;C:\Program Files\Git\bin

git config --global core.quotepath off
git config --global gui.encoding utf-8
git config --global i18n.commit.encoding utf-8
git config --global i18n.logoutputencoding utf-8
git config --global --add safe.directory C:/WealthCompas

echo Настройки кодировки Git успешно обновлены.
echo Теперь кириллические символы должны отображаться корректно.
pause 