FROM python:3.9

# RUN apt update && apt install -y procps gdb

# Add the `ls` alias to simplify debugging
RUN echo "alias ll='/bin/ls -l --color=auto'" >> /root/.bashrc

COPY ./requirements.txt ./conf/* /conf/

RUN pip3 install --no-cache-dir --upgrade pip && pip3 install --no-cache-dir -r /conf/requirements.txt


EXPOSE 8094

# Run the FastAPI application
CMD ["uvicorn", "./src/antares/main.py", "--port", "8094"]

