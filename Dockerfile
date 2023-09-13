# For more information, please refer to https://aka.ms/vscode-docker-python
FROM apache/spark-py:latest

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

USER root
