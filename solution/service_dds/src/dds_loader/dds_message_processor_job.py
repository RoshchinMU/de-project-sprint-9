from datetime import datetime
from logging import Logger
import uuid
from typing import Any, Dict, List

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                batch_size: int,
                logger: Logger,
                ) -> None:
      
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size


    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307'), name=str(obj))
    

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):

            # Читаем сообщение.
            msg_consumer = self._consumer.consume()
            
            if not msg_consumer: 
                break
            
            # раскладываем данные из kafka
            load_dt = datetime.now()
            load_src = f"kafka_topic_{self._consumer.topic}"

            user = {
                 "h_user_pk": self._uuid(msg_consumer["payload"]["user"]["id"]),
                 "user_id": msg_consumer["payload"]["user"]["id"],
                 "username": msg_consumer["payload"]["user"]["name"],
                 "userlogin": msg_consumer["payload"]["user"]["login"],
            }

            order = {
                 "h_order_pk": self._uuid(msg_consumer["payload"]["id"]),
                 "order_id": msg_consumer["payload"]["id"],
                 "cost": msg_consumer["payload"]["cost"],
                 "payment": msg_consumer["payload"]["payment"],
                 "status": msg_consumer["payload"]["status"],
                 "order_dt": msg_consumer["payload"]["date"],
            }

            restaurant = {
                 "h_restaurant_pk": self._uuid(msg_consumer["payload"]["restaurant"]["id"]),
                 "restaurant_id": msg_consumer["payload"]["restaurant"]["id"],
                 "restaurant_name": msg_consumer["payload"]["restaurant"]["name"],
            }

            products = list()

            for p in msg_consumer["payload"]["products"]:
                products.append({
                    "h_user_pk":                user["h_user_pk"],
                    "h_order_pk":               order["h_order_pk"],
                    "h_restaurant_pk":          restaurant["h_restaurant_pk"],
                    "h_product_pk":             self._uuid(p["id"]),
                    "product_id":               p["id"],
                    "product_price":            p["price"],
                    "product_quantity":         p["quantity"],
                    "product_name":             p["name"],
                    "h_category_pk":            self._uuid(p["category"]),
                    "category_name":            p["category"],
                    "hk_order_product_pk":      self._uuid(f'{order["order_id"]}#{p["id"]}'),
                    "hk_order_user_pk":         self._uuid(f'{order["order_id"]}#{user["user_id"]}'),
                    "hk_product_category_pk":   self._uuid(f'{p["id"]}#{p["category"]}'),
                    "hk_product_restaurant_pk": self._uuid(f'{p["id"]}#{restaurant["restaurant_id"]}'),
                })

            # грузим в postgres
            # hub
            self._dds_repository.insert_dds_hub(
                load_dt=load_dt,
                load_src=load_src,
                user=user,
                order=order,
                products=products,
                restaurant=restaurant
            )

            # # link
            self._dds_repository.insert_dds_link(
                load_dt=load_dt,
                load_src=load_src,
                products=products
            )

            # # sat
            self._dds_repository.insert_dds_sat(
                load_dt=load_dt,
                load_src=load_src,
                user=user,
                order=order,
                restaurant=restaurant,
                products=products
            )

            # отправляем в кафку
            msg_producer = {
                "user_id": user["user_id"],
                "data_cdm": [(
                                {"product_id":      p["product_id"],
                                 "product_name":    p["product_name"],
                                 "category_name":   p["category_name"],
                                 "order_cnt":       p["product_quantity"],}) for p in products]}
            self._producer.produce(msg_producer)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

