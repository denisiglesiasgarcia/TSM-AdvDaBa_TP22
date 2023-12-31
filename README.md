# TSM-AdvDaBa_TP22

TSM-AdvDaBa - Large database experiment with Neo4j

## Rendu du travail

**Informations selon les données moodle.**

| Nom | Valeur |
|:----------------------:|:-----------------------------|
| ID du groupe | IglWaeAdvDaBa23 |
| Namespace | adv-da-ba23-iglwae |
| ID du pod neo4j | neo4jlarge-deployment-6bb5bf5fdb-m9pw5 |
| Credentials neo4j | neo4j/testtest |
| ID du pod avec logs | python-app-deployment-777679b74f-7vcj6 |
| Temps du chargement | 0000 s |
| Liens git repository| <https://github.com/denisiglesiasgarcia/TSM-AdvDaBa_TP22.git> |
|Performance | {"team"="IglWaeAdvDaBa23", "N"=XX, "RAM_MB"="3800", "seconds"="YY"}|

Nous avons utilisé Python comme langague. Pour le développement local nous avons utilisé 2 containers docker qui fonctionnent avec docker-compose.
Pour kubernetes nous avons utilisé 2 deployments et 1 service. Un pour neo4j et un pour python. Ceux-ci communiquent entre eux avec un service.
L'approche utilisée consiste à charger le fichier JSON ligne par ligne, corriger les lignes contenant des valeurs non conformes (NumberInt/NaN), puis parser le JSON en streaming à l'aide de la bibliothèque ijson. Ensuite, nous avons créé une liste de dictionnaires contenant les articles et leurs références, ainsi qu'une liste de dictionnaires contenant les articles et leurs auteurs. Enfin, nous avons créé les nœuds et les relations correspondants dans Neo4j.

## Détails de l'implémentation

### Utilisation

Les identifiants de neo4j sont neo4j/testtest

#### Utilisation de Kubernetes

Utiliser les fichiers yaml dans le dossier kubernetes pour créer les deployments et services. Il faut aussi créer un namespace au préalable. [Lien vers les déploiements/services](https://github.com/denisiglesiasgarcia/TSM-AdvDaBa_TP22#deploymentsservices)

#### Variables d'environnement

| Variable              | Description                                                                                     |
|-----------------------|-------------------------------------------------------------------------------------------------|
| NEO4J_HOST            | Nom du service neo4j (localhost pour local ou nom du container pour docker-compose)            |
| NEO4J_PORT            | Port du service neo4j (7687)                                                                   |
| NEO4J_USER            | Nom d'utilisateur de neo4j (neo4j)                                                             |
| NEO4J_PASSWORD        | Mot de passe de neo4j (testtest)                                                               |
| NEO4J_URI             | URI de neo4j (bolt://localhost:7687)                                                           |
| JSON_FILE             | URL du fichier JSON (<http://vmrum.isc.heia-fr.ch/dblpv13.json>)                                   |
| BATCH_SIZE_ARTICLES   | Taille du batch d'articles juste après ijson (10000)                                            |
| BATCH_SIZE_APOC       | Taille du batch pour apoc lors du chargement des données dans neo4j (5000)                      |
| CHUNK_SIZE_HTTPX      | Taille du cache utilisé par httpx pour lire les lignes du fichier JSON                          |

### Docker

#### Debug app avec neo4j en local (docker-compose)

Remove all unused containers, networks, and images  
    ⚠️ WARNING! This will remove all images without at least one container associated to them.  

```bash
docker system prune -a
```

Lancer le container avec neo4j and python

```bash
docker compose -f docker-compose-local.yml up
```

#### Debug app avec neo4j en local (sans docker-compose)

Créer neo4j local avec apoc (ubuntu) pour tests

```bash
sudo docker run -p 7474:7474 -p 7687:7687 --name neo4j-apoc -e NEO4J_apoc_export_file_enabled=true -e NEO4J_apoc_import_file_enabled=true -e NEO4J_apoc_import_file_use__neo4j__config=true -e NEO4J_PLUGINS=\[\"apoc\"\] -e NEO4J_AUTH=neo4j/testtest neo4j:latest
```

#### App python pour k8s

Remove all unused containers, networks, and images  
    ⚠️ WARNING! This will remove all images without at least one container associated to them.  

```bash
docker system prune -a
```

Build container

```bash
docker build --no-cache -t comfy2665/neo4j_large .
```

Push l'image sur docker hub

```bash
docker push comfy2665/neo4j_large
```

### Kubernetes

#### Check list

- [x] Créer un namespace
- [x] Push l'image sur docker hub

#### Config

- Activer kubernetes dans Docker Desktop → Settings → Kubernetes → Enable Kubernetes
- Se logger sur le cluster et télécharger le fichier KubeConfig (en haut à droite de la page du cluster)
- Créer la variable d'environnement KUBECONFIG avec le path vers le fichier KubeConfig
  - Pour PowerShell:

    ```PowerShell
    $env:KUBECONFIG="C:\path\to\local.yaml
    ```

  - Pour CMD:

      ```cmd
      set KUBECONFIG=C:\path\to\local.yaml
      ```

  - Pour Linux:

      ```bash
      export KUBECONFIG=/path/to/local.yaml
      ```

- Créer un namespace
  - local (en dessous de la maison à gauche) → Projects/Namespaces → Create Namespace
- Test config

    ```bash
    kubectl describe ns adv-da-ba23-iglwae
    ```

#### Deployments/services

Deployments → Workloads → Deployments → Create Deployment
Services → Service Discovery → Services → Create Service

- Neo4j
  - Créer un deployment pour neo4j
    - Fichier YAML dans le dossier kubernetes `neo4j-deployment.yaml`
  - Créer un service pour neo4j
    - Fichier YAML dans le dossier kubernetes `neo4j-service.yaml`
- Python
  - Créer un deployment pour python
    - Fichier YAML dans le dossier kubernetes `python-app-deployment.yaml`

#### Monitoring

```bash
kubectl top pod -n adv-da-ba23-iglwae
```

#### Test de neo4j

```Cypher
kubectl port-forward -n adv-da-ba23-iglwae neo4jlarge-deployment-6444df6697-ckqw5 --address 0.0.0.0 7687:7687 7474:7474
```

```Cypher
// Number of unique article _id with title
MATCH (a:Article)
WHERE a._id IS NOT NULL AND a.title IS NOT NULL
RETURN COUNT(DISTINCT a._id) AS unique_article_id
```

Parquet: 5069313

```Cypher
MATCH (author:Author)-[:AUTHORED]->(article:Article {_id: '53e99784b7602d9701f3e15d'}),
      (article)-[:REFERENCES]->(refArticle:Article)
RETURN author, article, refArticle
LIMIT 100
```

## Ressources

- <https://neo4j.com/docs/operations-manual/current/docker/ref-settings/>
- <https://stackoverflow.com/questions/76207890/neo4j-docker-compose-to-kubernetes>
- <https://neo4j.com/docs/getting-started/cypher-intro/schema/>
- <https://neo4j.com/docs/cypher-manual/current/constraints/examples/>
