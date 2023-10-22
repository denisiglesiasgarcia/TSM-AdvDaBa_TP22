# TSM-AdvDaBa_TP22
TSM-AdvDaBa - Large database experiment with Neo4j


# TODO
- régler le problème de docker qui ne marche pas
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

