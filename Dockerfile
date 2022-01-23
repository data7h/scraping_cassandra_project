FROM python:3.6-alpine

RUN pip install --upgrade pip

RUN adduser -D houcine
USER houcine
WORKDIR /home/houcine

COPY --chown=houcine:houcine requirements.txt requirements.txt 
RUN pip install --user -r requirements.txt

ENV PATH="/home/houcine/.local/bin:${PATH}"

COPY --chown=houcine:houcine . .

CMD ["python", "API_OpenWeatherMap-hs.py", "runserver", "127.0.0.1:8000"]

# From the source image #python
#FROM python:3.6-slim 
# Identify maintainer
#LABEL maintainer = "hslimi@outlook.com"
# Set the default working directory
#WORKDIR /app/
#COPY API_OpenWeatherMap-hs.py requirements.txt city.list.json /app/
#RUN pip install -r requirements.txt
#CMD ["python","API_OpenWeatherMap-hs.py"]
# When the container starts, run this
#ENTRYPOINT python API_OpenWeatherMap-hs.py


    
