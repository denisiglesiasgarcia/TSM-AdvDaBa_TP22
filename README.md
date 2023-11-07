# TSM-AdvDaBa_TP22
TSM-AdvDaBa - Large database experiment with Neo4j

## Status
- le docker marche, le json est balancé dans neo4j
- Temps d'exécution actuel 4:25:23
- Mémoire utilisée par neo4j : 2.7GB/3GB
- Mémoire utilisée par python : 400MB/1GB

## TODO
- ~~ajouter un message de début/fin de script qui s'affiche dans le terminal avec le temps total~~
- ~~faire le nécessaire pour que le gros json ne soit pas en local~~
- ~~vérifier que le nom des articles est bien lié à son _id~~

## Commentaires
- Seulement les articles qui ont un article_id, article_title et author (dict avec _id et name) sont ajoutés à neo4j
- Seulement les articles qui ont un article_id, article_title et references (liste d'articles) sont ajoutés à neo4j

## How to use
- Modifier le [docker-compose.yml](docker-compose.yml) pour changer le path du gros fichier json vers celui de votre choix
- Build the docker image
    `docker build --no-cache -t neo4j_large .`
- Run the docker image
    `docker compose up`

## Debug
Stop the docker image  
    `docker compose down`

Remove all unused containers, networks, and images  
    ⚠️ WARNING! This will remove all images without at least one container associated to them.  
    `docker system prune -a`
