## Initialiser Consumer et Producer

## Configurer un Consumer Kafka
Configurer un consumer Kafka avec les paramètres nécessaires pour se connecter au cluster Kafka.

## Configurer un Producer Kafka
Configurer un producer Kafka avec les paramètres pour la production de messages.

## Souscrire Consumer au Topic Payment

Le consumer s'abonne au topic "payment" pour écouter les messages de statut de payment.

## Consommer les Messages du Topic Payment

Interroger continuellement pour de nouveaux messages sur le topic "payment".
Pour chaque message, décoder et extraire le statut de payment et l'ID de payment.

## Traiter le Statut de Payment

### Si le statut de payment est "done":
- Produire immédiatement un message au topic "order" avec le statut "done".

### Sinon (si le statut de payment n'est pas "done"):
- Entrer dans une boucle d'attente pour les mises à jour (jusqu'à 5 minutes):
  - Dans la boucle, interroger pour de nouveaux messages sur le topic "payment".
  - Vérifier si un nouveau message correspond au même ID de payment et a un statut mis à jour.
  - Si un statut mis à jour est trouvé et qu'il est "done", sortir de la boucle.

- Après la boucle (attente de 5 minutes ou une mise à jour trouvée):
  - Si une mise à jour a été trouvée et que le statut est "done", produire un message au topic "order" avec le statut "done".
  - Si aucune mise à jour n'a été trouvée ou si le statut n'est pas "done", produire un message au topic "order" avec le statut "failed".

## Produire le Message de l'Order

En fonction du statut final déterminé, produire un message au topic "order" indiquant le résultat ("done" ou "failed") avec l'ID de payment et un timestamp.
