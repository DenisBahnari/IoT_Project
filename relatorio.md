# Topicos Relatorio

# Docker

docker compose down -v
docker system prune -a
docker network prune
docker volume prune

sudo chown -R denis:denis /home/denis/Projects/IoT_Project/cloud_platform/data/mosquitto/

## Mosquitto

Deploy de mosquitto com docker image

Definicao do canal TLS com porto 8883

Geracao de certificados e chave privada com openssl

## Client Simulation Publish

Criacao de pasta para simular datasets do client

Usa certificado CA para estabelecer comunicacao segura TLS com MQTT

Cada detemrminado tempo publica uma row do CSV

## Client Simulation Publish

Definicao do container app que eh reposnsabilizado pela MQTT subscribe para obter info dos EVs online

Responsavel pelo processor que vai processar os MQTT subs

