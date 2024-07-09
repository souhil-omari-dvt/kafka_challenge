## Les topics existants :
J'ai créé un cluster Kafka sur Confluent Kafka, et j'ai mis en place 3 topics (checkout, payment et order).
- Format json de topic checkout (value): 
```bash
{
  "checkout_id": key, 
  "timestamp": current_datetime, 
  "status": "done",
  "user_id": "user_id",
  "items": [
    {
      "item_id": "item_identifier",
      "quantity": 1,
      "price": 12.99
    }
  ],
  "total_price": 12.99
}
``` 
- Format json de topic payment (value): 
```bash
{
    "checkout_id": checkout_id,
    "timestamp": current_datetime,
    "payment_status": payment_status,
    "total_price": 12.99
}
``` 
- Format json de topic order (value): 
```bash
{
    "payment_id": payment_id,
    "status": status,
    "timestamp": current_datetime
}
``` 

## Initialiser et Configurer kafka Consumer et Producer

- Configurer un Consumer Kafka avec les paramètres nécessaires pour se connecter au cluster Kafka.

- Configurer un Producer Kafka avec les paramètres pour la production de messages.

## Souscrire Consumer au Topic Payment

- Le consumer s'abonne au topic "payment" pour écouter les messages de statut de payment.

- Consommer les Messages du Topic Payment, Interroger continuellement pour de nouveaux messages sur le topic "payment".
Pour chaque message, décoder et extraire le statut de payment et l'ID de payment.

## Traiter le Statut de Payment
- Un message sur le topic checkout est généré, ce qui produit également un message payment avec le statut "waiting", en attendant que l'utilisateur effectue le paiement.

### Si le statut de payment est "done":
- Produire immédiatement un message au topic "order" avec le statut "done".
```python
produce_order_message(producer, order_topic, payment_id, "done")
``` 

### Sinon (si le statut de payment est "waiting"):
- Entrer dans une boucle d'attente pour les mises à jour (jusqu'à 5 minutes):
  - Dans la boucle, interroger pour de nouveaux messages sur le topic "payment".
  - Vérifier si un nouveau message correspond au même ID de payment et a un statut mis à jour.
  - Si un statut mis à jour est trouvé et qu'il est "done", sortir de la boucle.

- Après la boucle (attente de 5 minutes ou une mise à jour trouvée):
  - Si une mise à jour a été trouvée et que le statut est "done", produire un message au topic "order" avec le statut "done".
  - Si aucune mise à jour n'a été trouvée ou si le statut n'est pas "done", produire un message au topic "order" avec le statut "failed".

```python
    if updated_payment_status == "done":
        produce_order_message(producer, order_topic, payment_id, "done")
    else:
        # If no update or status is not done after 5 minutes, assume failure
        produce_order_message(producer, order_topic, payment_id, "failed")
``` 
## Produire le Message de l'Order

En fonction du statut final déterminé par les messages du topic payment, produire un message au topic "order" indiquant le résultat ("done" ou "failed") avec l'ID de payment et un timestamp.

# Installation et éxecution :
 
```bash
pip install confluent-kafka
``` 
```bash
python .\kafka_consumer.py
``` 
```bash
python .\kafka_payment.py
``` 
