FROM astronomerinc/ap-airflow:1.10.10-alpine3.10-onbuild
RUN apk update && apk add jq && pip install databricks-cli && pip install slack_sdk==3.7.0
ENV TARDIS_WHEEL_FILE=tardis-0.1.2-py3-none-any.whl
RUN wget --no-check-certificate https://tardis.conde.io/download/$TARDIS_WHEEL_FILE
RUN pip install $TARDIS_WHEEL_FILE
RUN rm $TARDIS_WHEEL_FILE