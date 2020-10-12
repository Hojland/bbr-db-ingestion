# Data science python template project
This is a standardized template for python projects in the data science chapter.  

## Quickstart

#### How to build and run
* Upon starting a new project build a docker image that serves as the base for all work. Run `make build_dev` and follow the instructions on the screen.
* To start working, launch a container using `make start_dev`. If you have a container that is active for this project, it will be stopped and removed before launching a new instance.
#### How to modify images/containers
* If you need to add any libraries or dependencies that are not python, add these to the *dev.Dockerfile* and rebuild the docker image using `make build_dev`. (E.g if you want to install Vim add the following line `RUN apt-get install -y vim`)
* If you need to add any Python libraries add these to the *requirements.txt* file and rebuild the docker image using `make build_dev`. Notice if you just want to test out a package quickly, simply install these inside the docker container (these packages are lost when the container is closed). You can enter a container by using the command `docker exec -it <CONTAINERNAME> bash` and install a package using `pip install <PACKAGENAME> --user`