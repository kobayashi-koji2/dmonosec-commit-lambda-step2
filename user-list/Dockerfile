FROM public.ecr.aws/lambda/python:3.12

COPY ./user-list/requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["lambda_function.lambda_handler"]
