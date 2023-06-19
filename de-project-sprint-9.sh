docker rm -v -f $(docker ps -qa) # удалить все контейнеры

docker compose up -d --build
docker compose down

docker tag dds_service:local cr.yandex/crp67d21pdq5g8eakfpk/service_dds:v2023-06-19-r1
docker push cr.yandex/crp67d21pdq5g8eakfpk/service_dds:v2023-06-19-r1

helm upgrade --install --atomic service-dds app -n c06-mihail-roszin

docker tag cdm_service:local cr.yandex/crp67d21pdq5g8eakfpk/service_cdm:v2023-06-19-r1
docker push cr.yandex/crp67d21pdq5g8eakfpk/service_cdm:v2023-06-19-r1

helm upgrade --install --atomic service-cdm app -n c06-mihail-roszin


kubectl get deployment
kubectl get pods -n c06-mihail-roszin

kubectl logs [-f] [-p] POD [-c CONTAINER]
kubectl logs service-dds-service-dds-app-7f4589989c-p5qn9
helm rollback service-dds 3 --namespace c06-mihail-roszin

kubectl delete deployment service-dds-service-dds-app # для удаления объектов