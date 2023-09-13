# For more information, please refer to https://aka.ms/vscode-docker-python
FROM apache/spark-py:latest

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1


USER root

# Install pip requirements
#COPY requirements.txt .
#RUN pip install -r requirements.txt

#WORKDIR /app
#sCOPY . /app

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
#RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
#USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
#CMD [ "pyspark" ]
#CMD ["python", "test_scripts.py"]