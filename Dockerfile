FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pip install pipenv
RUN pipenv install --deploy
COPY actions /app/actions
CMD ["uvicorn", "--host", "0.0.0.0", "actions.main:app"]
