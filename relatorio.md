# Topicos Relatorio

# Docker

docker compose down -v
docker system prune -a
docker network prune
docker volume prune

sudo chown -R denis:denis /home/denis/Projects/IoT_Project/data/

## Mosquitto

Deploy de mosquitto com docker image

Definicao do canal TLS com porto 8883

Geracao de certificados e chave privada com openssl

## Client Simulation Publish

Criacao de pasta para simular datasets do client

Usa certificado CA para estabelecer comunicacao segura TLS com MQTT

Cada detemrminado tempo publica uma row do CSV

