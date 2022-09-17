#docker ps 
cd .\mqttToDb 
docker buildx build \
--push \
--platform linux/arm64/v8,linux/amd64 \ --tag your-username/multiarch-example:latest .
