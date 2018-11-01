if [ -z "$1" ]
then
	version="test"
else
	version="$1"
fi
sudo docker rmi lenhattan86/my-kube-scheduler:$version -f
sudo docker rmi my-kube-scheduler:$version -f
# delete all containers
#docker rm -f $(docker ps -a -q)
# delete all images
#docker rmi -f $(docker images -q)

# compile Kuberntes first
#make quick-release
# make clean
make kube-scheduler

# build the scheduler docker image
kubernetes_src="."
#echo "FROM busybox
#ADD ./_output/dockerized/bin/linux/amd64/kube-scheduler /usr/local/bin/kube-scheduler" > $kubernetes_src/Dockerfile
docker build -t my-kube-scheduler:$version $kubernetes_src
docker tag my-kube-scheduler:$version lenhattan86/my-kube-scheduler:$version
# images may disappear before this command
docker push lenhattan86/my-kube-scheduler:$version
# upload my scheduler
echo $kubernetes_src $version

date