from datetime import datetime
from logging import Logger
import uuid
from typing import Any, Dict, List

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                dds_repository: CdmRepository,
                batch_size: int,
                logger: Logger,
                ) -> None:
      
        self._consumer = consumer
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

            user_id: uuid = self._uuid(self._uuid(msg_consumer["user_id"]))

            # cdm_user_product_counters
            for d in msg_consumer["data_cdm"]:

                product_id: uuid = self._uuid(self._uuid(d["product_id"]))
                product_name: str = d["product_name"]
                order_cnt: int = d["order_cnt"]

                self._dds_repository.insert_cdm_user_product_counters(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product_name,
                    order_cnt=order_cnt,
                )

            # insert_cdm_user_category_counters
            for d in msg_consumer["data_cdm"]:

                category_id: uuid = self._uuid(self._uuid(d["category_name"]))
                category_name: str = d["category_name"]
                order_cnt: int = d["order_cnt"]

                self._dds_repository.insert_cdm_user_category_counters(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=category_name,
                    order_cnt=order_cnt,
                )



        self._logger.info(f"{datetime.utcnow()}: FINISH")
