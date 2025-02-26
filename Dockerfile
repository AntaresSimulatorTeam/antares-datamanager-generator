FROM inca.rte-france.com/antares/python3.11-rte:1.1

# RUN apt update && apt install -y procps gdb

# Add the `ls` alias to simplify debugging
RUN echo "alias ll='/bin/ls -l --color=auto'" >> /root/.bashrc

WORKDIR /code/datamanager

COPY ./requirements.txt ./conf/* /conf/
COPY ./datamanager /code/datamanager
# Configure Python RTE mirrors
RUN echo "[global]" >> /etc/pip.conf &&\
    echo "   index = https://devin-depot.rte-france.com/repository/pypi-all" >> /etc/pip.conf &&\
    echo "   index-url = https://devin-depot.rte-france.com/repository/pypi-all/simple" >> /etc/pip.conf &&\
    echo "   trusted-host = devin-depot.rte-france.com" >> /etc/pip.conf

RUN pip3 install --no-cache-dir --upgrade pip && pip3 install --no-cache-dir -r /conf/requirements.txt


ENV PYTHONPATH="/code"

EXPOSE 8094

# Run the FastAPI application
#CMD ["uvicorn", "--app-dir", "/code", "datamanager.main:app", "--host", "0.0.0.0", "--port", "8094"]
CMD ["python3", "main.py"]