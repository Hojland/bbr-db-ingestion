project=$(notdir $(shell pwd))
host=$(shell hostname -I | grep -o ^[0-9.]*)
port=8888

.PHONY: build_dev build_prod setup start_dev start_prod start_dev_tf clean

build_dev:
	docker build -t $(project)_dev:latest -f docker/dev.Dockerfile .

build_prod:
	docker build -t $(project)_prod:latest -f docker/prod.Dockerfile .

start_dev:
	@-docker stop $(USER)-$(project)_dev > /dev/null 2>&1 ||:
	@-docker container prune --force > /dev/null

	@docker run \
		-u $(shell id -u):$(shell id -g) -it -d \
		-p $(port):8888 \
		--rm \
		--name $(USER)-$(project)_dev \
		-v $(PWD)/:/app/ $(project)_dev:latest bash > /dev/null

	@docker exec -it -d $(USER)-$(project)_dev bash \
		-c "jupyter lab --ip 0.0.0.0 --no-browser --NotebookApp.token=$(token)"

	@echo "Container started"
	@echo "Jupyter is running at http://$(host):$(port)/?token=$(token)"