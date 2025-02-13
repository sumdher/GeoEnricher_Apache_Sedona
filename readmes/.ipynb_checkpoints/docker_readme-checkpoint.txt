docker run -p 8888:8888 -p4140:4040 -p 8180:8080 -p 8181:8081 -v "./:/opt/workspace/my_data" -v "./z_volume:/opt/workspace" -e EXECUTOR_MEM=20g -e DRIVER_MEM=5g apache/sedona:latest

ssh -L 8888:localhost:8888 sudheer@172.20.27.4