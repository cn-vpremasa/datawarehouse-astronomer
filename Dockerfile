FROM astronomerinc/ap-airflow:1.10.10-alpine3.10-onbuild
ENV WHEEL_FILE=delibs-0.0.1-py3-none-any.whl
ENV WHEEL_DIRECTORY=dbfs:/dataeng-commons/delibs
RUN apk update && apk add jq && pip install databricks-cli==0.14.3 && pip install slack_sdk==3.7.0
ENV DATABRICKS_HOST=https://condenast-dev.cloud.databricks.com
RUN dbfs cp  $WHEEL_DIRECTORY/$WHEEL_FILE .
RUN pip install $WHEEL_FILE
RUN rm $WHEEL_FILE
ENV TARDIS_WHEEL_FILE=tardis-0.1.2-py3-none-any.whl
RUN wget --no-check-certificate https://tardis.conde.io/download/$TARDIS_WHEEL_FILE
RUN pip install $TARDIS_WHEEL_FILE
RUN rm $TARDIS_WHEEL_FILE
RUN pip install pandas==1.2.4
RUN pip install -U pandasql