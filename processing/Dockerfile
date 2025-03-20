FROM python:3
LABEL maintainer="cchan532@bcit.ca"

RUN mkdir /app
# We copy just the requirements.txt first to leverage Docker cache
# on `pip install`
COPY ./requirements.txt /app/requirements.txt

# Set the working directory
WORKDIR /app
# Install dependencies
RUN pip3 install -r requirements.txt
# Copy the source code
COPY . /app
# Change permissions and become a non-privileged user
RUN useradd -m katy
# Ensure log directory exists and has the right permissions
RUN mkdir -p /app/logs && chown -R katy:katy /app/logs

RUN chown -R katy:katy /app
USER katy
# Tells on which port the service listens in the container
EXPOSE 8100
# Entrypoint = run Python
ENTRYPOINT [ "python3" ]
# Default = run app.py
CMD [ "app.py" ]