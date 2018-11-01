echo "./debug.sh user@server scheduler_pod_name"
ssh $1 "kubectl logs --namespace=kube-system $2" > scheduler.log