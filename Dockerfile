# Start from Python 3.10 slim image
FROM python:3.10-slim

# Set environment variables
ENV USER user
ENV HOME /home/user
ENV VENV_PATH /home/user/venv

# Install required system packages
RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
    build-essential git vim nano screen

# Install MPI development libraries
RUN apt-get install -y libopenmpi-dev

# Add user
RUN adduser ${USER} --disabled-password --gecos "" && \
    mkdir -p ${HOME}

# Switch to user
USER ${USER}
WORKDIR ${HOME}

# Create a virtual environment
RUN python3 -m venv ${VENV_PATH}
ENV PATH="${VENV_PATH}/bin:$PATH"

# Install Python packages
RUN pip install jupyter numpy networkx flake8 redis==4.4.2
RUN pip install mpi4py

# Clone and install dispel4py from the specific Git repository
RUN git clone https://github.com/StreamingFlow/d4py.git && \
    cd dispel4py && python setup.py install

