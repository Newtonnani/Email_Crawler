FROM public.ecr.aws/lambda/python:3.8

# Installing python dependencies
COPY ./worker-agent/requirements.txt  ./
RUN pip3 install --no-cache-dir -r requirements.txt

RUN yum update -y glib2
#RUN yum update -y
# Copying init to Container
#COPY ./worker-agent/__init__.py ./

# Copying src code to Container
COPY ./worker-agent/* ./

RUN chmod +x ./

# Running Python Application
CMD ["app.handler"]
