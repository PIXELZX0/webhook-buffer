FROM python:3.13-alpine
WORKDIR /app
COPY receiver.py sender.py ./
# APP=receiver 또는 APP=sender
ENV APP=receiver PYTHONUNBUFFERED=1
CMD ["sh", "-c", "exec python /app/$APP.py"]
