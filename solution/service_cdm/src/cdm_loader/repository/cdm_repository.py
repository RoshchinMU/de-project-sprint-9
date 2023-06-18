import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_cdm_user_product_counters(self,
                       user_id: uuid,
                       product_id: uuid,
                       product_name: str,
                       order_cnt: int) -> None:
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""insert into cdm.user_product_counters
                            (    user_id,    product_id,    product_name,   order_cnt  )
                        values('{user_id}','{product_id}','{product_name}',{order_cnt})
                        ON CONFLICT (user_id, product_id) DO NOTHING;"""
                )
   
            
    def insert_cdm_user_category_counters(self,
                       user_id: uuid,
                       category_id: uuid,
                       category_name: str,
                       order_cnt: int) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:         
                 cur.execute(
                    f"""insert into cdm.user_category_counters
                            (    user_id,    category_id,    category_name,   order_cnt  )
                        values('{user_id}','{category_id}','{category_name}',{order_cnt})
                        ON CONFLICT (user_id, category_id) DO NOTHING;"""
                )
