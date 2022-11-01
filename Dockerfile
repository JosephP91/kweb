FROM python:3.10

EXPOSE 8000

RUN mkdir -p /usr/src/kweb
WORKDIR /usr/src/kweb

COPY requirements.txt /usr/src/kweb
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "kweb/main.py"]

