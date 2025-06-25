FROM python:3
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt
EXPOSE 8050
CMD ["python", "main.py"]