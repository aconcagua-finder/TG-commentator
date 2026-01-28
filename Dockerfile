FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем проект
COPY . .

# По умолчанию ничего не запускаем — команда будет задана в docker-compose
CMD ["python", "placeholder.py"]
