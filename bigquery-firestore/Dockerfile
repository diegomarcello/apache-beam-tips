FROM apache/beam_python3.7_sdk:2.28.0

# Install dependencies of gmpy2
RUN apt-get update && \
	apt-get install -y libgmp-dev libmpfr-dev libmpc-dev

RUN pip install google-cloud-firestore

