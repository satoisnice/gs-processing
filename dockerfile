FROM python:3

LABEL maintainer="cbell86@my.bcit.ca"

RUN mkdir /app

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip3 install -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8090

ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]