# Topicos Relatorio

# Docker

docker compose up --build

docker compose down -v
docker system prune -a
docker network prune
docker volume prune

docker system prune -a --volumes -f


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

## APP

container de docker com mqtt subscriver e procesor

mqtt subscriber, classe que serve para escutar o dataset online

processor carrega os dados offline, e quardaos logo na BD

processa os dados online que vem a cada 2s

## DB

Escolhido o postgres por ser uma base de dado que ja estamos familiarizados

Configuraca da bd com o docker

Definicao das tabelas 

Para inserir dados offline na tabela deciduise guardar primeiro as estacoes em memoria de dicionario 
e depois adicionar repsetivamente apenas as necessarias ja que existem muitas estacoes +/- 30000

Depois todas as sessoes online vao sendo inseridas na BD semrque que o MQTT as recebe.

# ML Processor

Definido o ml processor.py como um servidor via Flask

