FROM harbor.aws.c.dk/datascience/base/python-dev:latest
COPY requirements.txt /app
# COPY resources/ /resources/
COPY src/ /app/src/
RUN pip install -r requirements.txt
RUN pip install jupyterlab
WORKDIR /app/src/
CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --no-browser --NotebookApp.token=mojn --allow-root"]
