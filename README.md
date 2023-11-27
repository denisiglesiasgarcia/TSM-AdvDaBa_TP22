# TSM-AdvDaBa_TP22

TSM-AdvDaBa - Large database experiment with Neo4j

## Status

- Temps d'éxécution 38 minutes / 1.8GB RAM (Python 100MB) / 2-3 CPU / Batch_size_articles = 1000 / neo4j_batch_size = 100
![Test avec approche url](image-1.png)

## TODO

- ~~ajouter un message de début/fin de script qui s'affiche dans le terminal avec le temps total~~
- ~~faire le nécessaire pour que le gros json ne soit pas en local~~
- ~~vérifier que le nom des articles est bien lié à son _id~~
- ~~enlever neo4j du container pour kubernetes~~
- ~~Déploiement sur kubernetes fonctionnel~~
- Tester temps d'exécution script sur kubernetes
- Tester approche avec url au lieu de pvc

## Commentaires

- Seulement les articles qui ont un article_id, article_title et author (dict avec _id et name) sont ajoutés à neo4j
- Seulement les articles qui ont un article_id, article_title et references (liste d'articles) sont ajoutés à neo4j

## Docker

### Debug app avec neo4j en local

Remove all unused containers, networks, and images  
    ⚠️ WARNING! This will remove all images without at least one container associated to them.  

```
docker system prune -a
```

Lancer le container avec neo4j and python

```
docker compose -f docker-compose-local.yml up
```

Créer neo4j local avec apoc (ubuntu) pour tests

```
sudo docker run -p 7474:7474 -p 7687:7687 --name neo4j-apoc -e NEO4J_apoc_export_file_enabled=true -e NEO4J_apoc_import_file_enabled=true -e NEO4J_apoc_import_file_use__neo4j__config=true -e NEO4J_PLUGINS=\[\"apoc\"\] -e NEO4J_AUTH=neo4j/testtest neo4j:latest
```

### App python pour k8s

Remove all unused containers, networks, and images  
    ⚠️ WARNING! This will remove all images without at least one container associated to them.  

```
docker system prune -a
```

Build container

```
docker build --no-cache -t comfy2665/neo4j_large .
```

Push l'image sur docker hub

```
docker push comfy2665/neo4j_large
```

## Kubernetes

### Check list

- [x] Créer un namespace
- [x] Push l'image sur docker hub

### Config

- Activer kubernetes dans Docker Desktop → Settings → Kubernetes → Enable Kubernetes
- Se logger sur le cluster et télécharger le fichier KubeConfig (en haut à droite de la page du cluster)
- Créer la variable d'environnement KUBECONFIG avec le path vers le fichier KubeConfig
  - Pour PowerShell:

        ```
        $env:KUBECONFIG="C:\path\to\local.yaml
        ```
  - Pour CMD:

        ```
        set KUBECONFIG=C:\path\to\local.yaml
        ```
  - Pour Linux:

        ```
        export KUBECONFIG=/path/to/local.yaml
        ```

- Créer un namespace
  - local (en dessous de la maison à gauche) → Projects/Namespaces → Create Namespace
- Test config

    ```
    kubectl describe ns adv-da-ba23-iglwae
    ```

### JSON (pour information, pas nécessaire avec approche par url)

- Créer un persistent volume claim (pvc) → Storage → Persistent Volume Claims → Create Persistent Volume Claim
  - Fichier YAML dans le dossier kubernetes `json-data.yaml`
- Créer un pod temporaire pour envoyer le fichier dessus
  - Fichier YAML dans le dossier kubernetes `temp-pod.yaml`

    ```
    kubectl apply -f temp-pod.yaml
    ```

- Copier le fichier JSON

    ```
    cd "C:\Users\denis.iglesias\OneDrive - HESSO\03 Master\01 Cours\12 TSM-AdvDaBa\02 Labo\03 Labo 2.2 neo4j large database"
    kubectl cp dblpv13.json adv-da-ba23-iglwae/temp-pod:/mnt/dblpv13.json
    ```

- Vérifier le fichier

    ```
    kubectl exec -it -n adv-da-ba23-iglwae temp-pod -- ls -lh /mnt/dblpv13.json
    ```

- Effacer le pod temporaire

    ```
    kubectl delete pod -n adv-da-ba23-iglwae temp-pod
    ```

### Deployments/services

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

### Monitoring

```
kubectl top pod -n adv-da-ba23-iglwae
```

### Test de neo4j

```
kubectl port-forward -n adv-da-ba23-iglwae neo4jlarge-deployment-6444df6697-ckqw5 --address 0.0.0.0 7687:7687 7474:7474
```

```
MATCH (author:Author)-[:AUTHORED]->(article:Article {_id: '53e99b2cb7602d97023c84e9'}),
      (article)-[:REFERENCES]->(refArticle:Article)
RETURN author, article, refArticle
LIMIT 100
```

```
MATCH (article:Article)-[:REFERENCES]->(ref:Article)
RETURN COUNT(DISTINCT article)
```

```
MATCH (author:Author)-[:AUTHORED]->(a:Article)
RETURN author.name, author.`_id`, a.`_id`, a.title
LIMIT 10
```

## Ressources

<https://neo4j.com/docs/operations-manual/current/docker/ref-settings/>
<https://stackoverflow.com/questions/76207890/neo4j-docker-compose-to-kubernetes>
<https://neo4j.com/docs/getting-started/cypher-intro/schema/>
