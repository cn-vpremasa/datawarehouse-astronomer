FROM astronomerinc/ap-airflow:1.10.10-alpine3.10-onbuild
RUN apk update && apk add jq && pip install databricks-cli==0.14.3 && pip install slack_sdk==3.7.0
