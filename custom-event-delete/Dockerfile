FROM public.ecr.aws/lambda/python:3.12

COPY ./_local-dev-files/layer/common_functions_layer/python /opt/python
COPY ./custom-event-delete/requirements.txt  .
RUN  pip3 install -r requirements.txt --target /opt/python

RUN rm /etc/dnf/vars/releasever
RUN dnf --refresh update --releasever=2023.6.20241031 -y

COPY ./custom-event-delete/contents ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.lambda_handler"]
