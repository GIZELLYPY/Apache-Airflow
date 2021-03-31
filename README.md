# Apache-Airflow
A DAG  consome duas APIS, unifica seus retornos e salva no banco de dados postgres e depois faz um select no qual eu poderia por exemplo gerar um json ou um txt. A forma de fazer request também é através de um operator e os dados da url ficam configurados no Airflow>Connections assim como os dados de conexão ao banco. 
