FROM apache/airflow:2.4.3

USER root
# We need wget to set up the PPA and xvfb to have a virtual screen and unzip to install the Chromedriver
RUN apt-get update && apt-get install -y wget xvfb unzip

# Set up the Chrome PPA
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list

# Update the package list and install chrome
RUN apt-get update -y
RUN apt-get install -y google-chrome-stable

# Put Chromedriver into the PATH
ENV PATH $CHROMEDRIVER_DIR:$PATH


USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt 