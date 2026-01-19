FROM python:3.13-slim

WORKDIR /app

# Копируем зависимости (если есть requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем оба скрипта
COPY commentator.py admin_bot.py config.ini accounts.json ./

# По умолчанию ничего не запускаем — команда будет задана в docker-compose
CMD ["python", "placeholder.py"]