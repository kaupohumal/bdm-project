# How to run

First bring up the docker container with `docker compose up`. Then copy the relevant data into the work/data folder (2009.csv and 2010.csv). Then connect to Jupyter and run the notebook as normal on [`http://localhost:8888`](http://localhost:8888).

If there is an issue with the source files not being found, modify the docker compose file to mount into `- "./work/data:/home/lab_data"` instead.
