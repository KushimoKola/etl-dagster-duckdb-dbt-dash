## Create virtual environment
`python -m venv dagst`

### Active virtual environment
`source dagst/bin/activate`

## Install Dagster
`pip install dagster`

### Install dagster and webservice
`pip install dagster dagster-webserver --find-links=https://github.com/dagster-io/build-grpcio/wiki/Wheels`

## Create dagster project
`dagster project scaffold --name sky-soccer-data`

### 🚨 If you have an error 🚨 while trying to create your project, run the following:
`pip uninstall grpcio`
`export GRPC_PYTHON_LDFLAGS=" -framework CoreFoundation"`
`pip install grpcio --no-binary :all:`


## Configure `setup.py` and `assets.py`
[configuration](configure-setup-assets.ipynb)

### cd into project folder
`cd sky-soccer-data`

### install all requirements.
`pip install -e ".[dev]"`

## Configure the @asset decorator
`dagster dev`

The server should be running on this link

`http://127.0.0.1:3000`

click on `materialize` so the process can act as instructed