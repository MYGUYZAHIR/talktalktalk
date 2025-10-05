FROM python:2.7

FROM python:3.11-slim

WORKDIR /opt/app/talktalktalk

# Install dependencies (including python-chess)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Update Config For Docker bind host
RUN sed -i "s/HOST =.*/HOST = \"0\.0\.0\.0\"/g" config.py

EXPOSE 9000
CMD ["python", "talktalktalk.py"]
