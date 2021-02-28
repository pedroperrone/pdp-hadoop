## Análise de logs com o Hadoop

Este repositório contém a implementação do traalho prático da disciplina de Processamento Distribuído e Paralelo da Universidade Federal do Rio Grande do Sul.

## Configuração do cluster

O cluster foi configurado com base [nesta publicação](https://clubhouse.io/developer-how-to/how-to-set-up-a-hadoop-cluster-in-docker/). Ela utiliza `docker-compose` para gerenciar os múltiplos containers que compõem o cluster.

## Como rodar

Para levantar o cluster, rode `docker-compose up -d`. Ao rodar `docker ps`, múltiplos containers relacionados ao hadoop devem ser listados. Para parar o cluster, rode `docker-compose down`.

## Código

O código está sob o path `app`. Há um volume no container `namenode` que linka o diretório do container ao da máquina física. Sendo assim, o código pode ser editado na sua máquina e executado sempre non container.

Para gerar um `.jar`, acesse o container com `docker exec -it namenode bash`, navegue para `app/` e rode `mvn package`. O arquivo estará em `target/
