# Инструкция по развертыванию приложения в Streamlit Cloud

Это руководство описывает процесс публикации приложения анализа недвижимости Дубая на Streamlit Community Cloud.

## Подготовка к деплою

1. Убедитесь, что ваш репозиторий содержит следующие файлы:
   - `streamlit_app.py` - основной файл приложения
   - `requirements-streamlit.txt` - зависимости для Streamlit-приложения
   - `.streamlit/secrets.toml` - файл с секретами (НЕ добавляйте его в Git!)
   - `.streamlit/config.toml` - конфигурация Streamlit

2. Создайте аккаунт на GitHub, если у вас его еще нет.

3. Загрузите ваш код в GitHub-репозиторий:
   ```bash
   git init
   git add streamlit_app.py requirements-streamlit.txt .streamlit/config.toml README_STREAMLIT_CLOUD.md
   git commit -m "Подготовка для Streamlit Cloud"
   git remote add origin https://github.com/username/your-repo-name.git
   git push -u origin main
   ```

## Развертывание на Streamlit Cloud

1. Перейдите на [share.streamlit.io](https://share.streamlit.io/) и войдите с помощью вашего GitHub-аккаунта.

2. Нажмите кнопку "New app".

3. В форме создания приложения:
   - Выберите ваш репозиторий
   - Выберите ветку (обычно `main`)
   - Укажите путь к основному файлу приложения: `streamlit_app.py`
   - В разделе "Advanced settings" укажите имя файла с зависимостями: `requirements-streamlit.txt`
   - Нажмите "Deploy!"

4. Дождитесь завершения процесса развертывания.

## Настройка секретов

После развертывания необходимо настроить секреты для подключения к базе данных:

1. В Streamlit Cloud перейдите на страницу вашего приложения
2. Нажмите на настройки (иконка ⚙️)
3. Выберите "Secrets"
4. Введите содержимое вашего файла `.streamlit/secrets.toml`
5. Сохраните изменения

## Обновление приложения

Для обновления приложения просто внесите изменения в ваш код и отправьте их в GitHub:

```bash
git add .
git commit -m "Обновление приложения"
git push
```

Streamlit Cloud автоматически обнаружит изменения и перезапустит ваше приложение.

## Использование демонстрационного режима

Если в Streamlit Cloud у вас нет доступа к вашей базе данных PostgreSQL, приложение автоматически перейдет в демонстрационный режим и будет отображать тестовые данные. Это позволяет показывать функциональность приложения даже без подключения к реальной базе данных.

## Примечания о базе данных

Для работы с реальной базой данных в Streamlit Cloud:

1. Убедитесь, что ваша база данных PostgreSQL доступна из интернета
2. Настройте правильные параметры подключения в секретах
3. Используйте SSL для безопасного подключения

Альтернативно, вы можете использовать облачные базы данных, такие как:
- Supabase
- Neon
- ElephantSQL
- Amazon RDS
- Google Cloud SQL

## Дополнительные ресурсы

- [Документация Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud)
- [Управление секретами в Streamlit](https://docs.streamlit.io/streamlit-community-cloud/deploy-your-app/secrets-management) 