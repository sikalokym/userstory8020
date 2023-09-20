# userstory8020

## Please follow next steps to run the test:
  -  Clone the repo, go to the root directory of the repo.
  -  Build an image using DockerFile with is located in the root directory of the repo, the image contains a Spark server:

          $ docker build -f "Dockerfile" -t userstory8020:latest "."

  - Run a container using created above image: (win cmd example)

          $ docker run -it -p 4040:4040 -p 15002:15002 -v %cd%/source_data:/usr/src/userStory8020/source_data -v %cd%/client_data:/usr/src/userStory8020/client_data userstory8020:latest bash
    In the command the default 15002 port is opened for the abalilty to connect to the Spark server outside the container. Two volumes are mounted for source and client data.
    
  - In the container run the spark connect server:

          $ /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0

  - In the container install package:

          $ pip install userstory8020

  - In the container run the app with the following parameters:

          $ userstory8020 -sp "sc://localhost:15002" -prc "/usr/local/lib/python3.10/dist-packages/userstory8020/process.json"

    > Output of the command will show you a process of the run, for example:

    ![image](https://github.com/sikalokym/userstory8020/assets/80965401/b6eecb1b-3fa5-4935-b2b6-daf1c85468a9)

    > More detailed info of the process you will find in LOG files which are located in /tmp/userstory8020/logs directory.
