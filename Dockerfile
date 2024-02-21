FROM python:3.12
COPY . /fixer
WORKDIR /fixer
RUN pip install pipenv &&\
    pipenv install --deploy --ignore-pipfile
ENTRYPOINT [ "pipenv", "run", "fixer" ]