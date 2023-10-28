# TSM-AdvDaBa_TP22
TSM-AdvDaBa - Large database experiment with Neo4j

# Status
- le docker marche pour l'instant, les json est balancé dans neo4j
- Mettre le json dans la racine du dossier

# TODO
- ajouter un message de début/fin de script qui s'affiche dans le terminal avec le temps total
- faire le nécessaire pour que le gros json ne soit pas en local

# How to use
Build the docker image
`docker build -t neo4j_large .`

Run the docker image
`docker compose up`

## Debug
Stop the docker image
`docker compose down`

Remove all stopped docker images
WARNING! This will remove all stopped containers.
`docker container prune`

