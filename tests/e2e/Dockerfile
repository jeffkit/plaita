FROM python:3.12-slim

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir -e ".[all,dev]"

CMD ["python", "-c", "import plaita; print('plaita imported successfully')"]
