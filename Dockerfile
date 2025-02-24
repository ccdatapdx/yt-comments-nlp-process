FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY yt_channel_process_nlp.py file_processing.py lambda_function.py ${LAMBDA_TASK_ROOT}/

RUN python -m spacy download en_core_web_trf

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)

CMD [ "lambda_function.lambda_handler" ]