start_dev:
	$(eval export FOLDERNAME=$(shell basename "${PWD}"))
	@-docker stop $(FOLDERNAME)_dev > /dev/null 2>&1 ||:
	@-docker container prune --force > /dev/null
	docker login -u 'niivnuuday'
	@-docker build -f Dockerfile . \
		 -t $(FOLDERNAME)_dev:latest
	@-docker run \
		-p 9998:8888 \
		--env-file .env \
		--rm \
		-v $(PWD)/:/app/ \
		--name $(FOLDERNAME)_dev \
		--cpus=1 \
		-d \
		$(FOLDERNAME)_dev:latest > /dev/null
	@echo "Container started"
	@echo "Jupyter is running at http://localhost:9998/?token=mojn"
