FROM astronomerinc/ap-airflow:1.10.10-alpine3.10-onbuild
ENV WHEEL_FILE=delibs-0.0.1-py3-none-any.whl
ENV WHEEL_DIRECTORY=dbfs:/dataeng-commons/delibs
ENV DATABRICKS_TOKEN=dapi116bcf994bf84e0869c709606b931bd9
ENV DATABRICKS_HOST=https://condenast.cloud.databricks.com
RUN pip install databricks-cli
RUN dbfs cp  $WHEEL_DIRECTORY/$WHEEL_FILE .
RUN pip install $WHEEL_FILE
RUN rm $WHEEL_FILE
ENV TARDIS_WHEEL_FILE=tardis-0.1.2-py3-none-any.whl
RUN wget --no-check-certificate https://tardis.conde.io/download/$TARDIS_WHEEL_FILE
RUN pip install $TARDIS_WHEEL_FILE
RUN rm $TARDIS_WHEEL_FILE
RUN pip install pandas==1.2.4
RUN pip install -U pandasql