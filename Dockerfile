FROM inca.rte-france.com/antares/python3.11-rte:1.1

# RUN apt update && apt install -y procps gdb

# Add the `ls` alias to simplify debugging
RUN echo "alias ll='/bin/ls -l --color=auto'" >> /root/.bashrc

WORKDIR /code

COPY ./requirements.txt ./conf/* /conf/

RUN pip3 install --no-cache-dir --upgrade pip && pip3 install --no-cache-dir -r /conf/requirements.txt

# Copy the application source code
COPY ./datamanager/* /code/

ENV PYTHONPATH="/code"

EXPOSE 8094

# Run the FastAPI application
CMD ["uvicorn", "datamanager.main:app", "--host", "0.0.0.0", "--port", "8094"]