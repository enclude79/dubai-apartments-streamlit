# Инструкция по деплою в Streamlit Cloud

## Шаг 1: Проверка файлов в репозитории

Убедитесь, что в вашем репозитории есть следующие файлы:
- `streamlit_sqlite_app.py` (основной файл приложения)
- `dubai_properties.db` (файл базы данных)
- `requirements.txt` (зависимости)

## Шаг 2: Настройка на странице деплоя Streamlit

1. Перейдите на https://share.streamlit.io/deploy
2. В поле "Repository" введите точное название репозитория: `enclude79/dubai-apartments-streamlit`
3. В поле "Main file path" введите точное имя файла: `streamlit_sqlite_app.py` (НЕ streamlit_app.py)
4. При необходимости нажмите на "Advanced settings" и выберите версию Python 3.10 или 3.11
5. Нажмите кнопку "Deploy"

## Шаг 3: Проверка работы приложения

После успешного деплоя вы будете перенаправлены на страницу с вашим приложением.
Приложение будет доступно по адресу: `https://enclude79-dubai-apartments-streamlit.streamlit.app/`

## Возможные проблемы и их решения:

### Проблема: "This branch does not exist"
**Решение:** Убедитесь, что репозиторий публичный и имя репозитория введено правильно.

### Проблема: "This file does not exist"
**Решение:** Проверьте точное имя файла в репозитории. Используйте `streamlit_sqlite_app.py` вместо `streamlit_app.py`.

### Проблема: Ошибка с зависимостями
**Решение:** Проверьте содержимое файла `requirements.txt` и убедитесь, что все необходимые библиотеки перечислены.

## Полезные ссылки:
- [Документация Streamlit по деплою](https://docs.streamlit.io/streamlit-community-cloud/deploy-an-app)
- [Troubleshooting деплоя в Streamlit](https://docs.streamlit.io/streamlit-community-cloud/troubleshooting) 