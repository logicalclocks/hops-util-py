all: rm-image build

# Build the hops container
# Note - the build process can be memory intensive due to
# pip. If the install process is killed midway through,
# rerun it and check `docker stats` during execution; you may
# need to increase Docker's memory limits.

build:
	cd .. && docker build . \
	-f ./docker/Dockerfile \
	--tag hops


# Run a container to serve as a development workspace with
# the local repo mounted into it. IMPORTANT: this gives
# the container access to the host's filesystem!
run:
	docker run \
	-dt \
	-v `pwd`/..:/hops \
	--name hops \
	hops

# Create a shell into a running container
shell:
	docker exec -it hops /bin/bash

# Stop and remove running containers
clean:
	-docker stop hops
	-docker rm hops

# Expunge old container image
rm-image:
	-docker rmi hops