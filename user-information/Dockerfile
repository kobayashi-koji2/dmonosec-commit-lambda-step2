FROM public.ecr.aws/lambda/python:3.12

COPY ./_local-dev-files/layer/common_functions_layer/python /opt/python
COPY ./user-information/requirements.txt  .
RUN  pip3 install -r requirements.txt --target /opt/python

COPY ./user-information/contents ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.lambda_handler"]
