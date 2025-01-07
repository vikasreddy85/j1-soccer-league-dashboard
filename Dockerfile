FROM prefecthq/prefect:3.1-python3.12 

WORKDIR /app

COPY . /app

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows

EXPOSE 4200