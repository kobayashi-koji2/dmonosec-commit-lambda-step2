FROM public.ecr.aws/lambda/python:3.9

COPY ./receivedata-2/requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["lambda_function.lambda_handler"]
