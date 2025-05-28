# Инструкция по деплою приложения в Streamlit Cloud

## 1. Добавление файла requirements.txt в репозиторий

1. Откройте ваш репозиторий на GitHub: https://github.com/enclude79/dubai-apartments-streamlit
2. Нажмите кнопку "Add file" и выберите "Upload files"
3. Перетащите файл `requirements.txt` из локальной папки в браузер
4. Добавьте комментарий к коммиту: "Добавление requirements.txt для Streamlit Cloud"
5. Нажмите "Commit changes"

## 2. Деплой приложения в Streamlit Cloud

1. Перейдите на https://share.streamlit.io/deploy
2. В поле "Repository" введите: `enclude79/dubai-apartments-streamlit`
3. В поле "Main file path" введите: `streamlit_sqlite_app.py` (НЕ streamlit_app.py)
4. Нажмите кнопку "Deploy"

## 3. Проверка работы приложения

После успешного деплоя приложение будет доступно по адресу:
https://enclude79-dubai-apartments-streamlit.streamlit.app/

## Важные примечания

- Кракозябры в интерфейсе Git не влияют на работу приложения
- Убедитесь, что в репозитории есть все необходимые файлы:
  - streamlit_sqlite_app.py
  - dubai_properties.db
  - requirements.txt
- Если какого-то файла нет, добавьте его через интерфейс GitHub 