# kafka-alura-formation
Following Kafka formation from alura.com.br

Algumas alterações foram feitas em relação ao curso original. 

### Kafka e zookeper
Ao invés de usar uma instalação local do kafka
adicionei um arquivo docker-compose para o kafka e zookeeper

Para executar basta:

    docker-compose up -d

as pastas de configuração do kafka e do zookeeper estão mapeadas para
kafka_data e zookeeper_data respectivamente.

### Http Server
Ao invés de implementar um servidor http como feito no curso, optei por criar
um serviço fake http, que ao ser executado simula o que seria feito na request.

