FROM docker.io/library/python:3-alpine as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

FROM base AS python-deps

# Install pipenv and compilation dependencies
RUN apk add --no-cache build-base
RUN pip install pipenv

# Install project dependencies 
RUN apk add --no-cache libffi-dev

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy


FROM base AS runtime

# Copy virtual env from python-deps stage
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

ARG UID=1012
ARG GID=1012

RUN addgroup -S -g $GID vue_importer
RUN adduser  -S -g $GID -u $UID -h /opt/vue_importer vue_importer

USER $UID
WORKDIR /opt/vue_importer

# Install application into container
COPY *.py .

# Run the application
ENTRYPOINT ["python", "vue-importer.py"]