import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db



    def insert_dds_hub(self,
                       load_dt: datetime,
                       load_src: str,
                       user: dict,
                       order: dict,
                       restaurant: dict,
                       products: dict) -> None:
        
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""insert into dds.h_user
                            (        h_user_pk,            user_id,      load_dt,    load_src  )
                        values('{user["h_user_pk"]}','{user["user_id"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_user_pk) DO NOTHING;"""
                )
                
                cur.execute(
                    f"""insert into dds.h_order
                              (         h_order_pk,            order_id,            order_dt,      load_dt,    load_src  )
                        values('{order["h_order_pk"]}',{order["order_id"]},'{order["order_dt"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_order_pk) DO NOTHING;"""
                )
        
                cur.execute(
                    f"""insert into dds.h_restaurant
                              (              h_restaurant_pk,                  restaurant_id,      load_dt,    load_src  )
                        values('{restaurant["h_restaurant_pk"]}','{restaurant["restaurant_id"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;"""
                )

                
                for p in products: # грузим список
                    cur.execute(
                        f"""insert into dds.h_category
                                  (     h_category_pk,         category_name,      load_dt,    load_src  )
                            values('{p["h_category_pk"]}','{p["category_name"]}','{load_dt}','{load_src}')
                            ON CONFLICT (h_category_pk) DO NOTHING;"""
                    )

                    cur.execute(
                        f"""insert into dds.h_product
                                  (     h_product_pk,         product_id,      load_dt,    load_src  )
                            values('{p["h_product_pk"]}','{p["product_id"]}','{load_dt}','{load_src}')
                            ON CONFLICT (h_product_pk) DO NOTHING;"""
                )
              
            

    def insert_dds_link(self,
                       load_dt: datetime,
                       load_src: str,
                       products: dict) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:         
                for p in products: # грузим список
                    cur.execute(
                        f"""insert into dds.l_order_product
                                  (     hk_order_product_pk,         h_order_pk,         h_product_pk,      load_dt,    load_src  )
                            values('{p["hk_order_product_pk"]}','{p["h_order_pk"]}','{p["h_product_pk"]}','{load_dt}','{load_src}')
                            ON CONFLICT (hk_order_product_pk) DO NOTHING;"""
                    )

                    cur.execute(
                        f"""insert into dds.l_order_user
                                  (     hk_order_user_pk,         h_order_pk,         h_user_pk,      load_dt,    load_src  )
                            values('{p["hk_order_user_pk"]}','{p["h_order_pk"]}','{p["h_user_pk"]}','{load_dt}','{load_src}')
                            ON CONFLICT (hk_order_user_pk) DO NOTHING;"""
                    )

                    cur.execute(
                        f"""insert into dds.l_product_category
                                  (     hk_product_category_pk,         h_product_pk,         h_category_pk,      load_dt,    load_src  )
                            values('{p["hk_product_category_pk"]}','{p["h_product_pk"]}','{p["h_category_pk"]}','{load_dt}','{load_src}')
                            ON CONFLICT (hk_product_category_pk) DO NOTHING;"""
                    )

                    cur.execute(
                        f"""insert into dds.l_product_restaurant
                                  (     hk_product_restaurant_pk,         h_product_pk,         h_restaurant_pk,      load_dt,    load_src  )
                            values('{p["hk_product_restaurant_pk"]}','{p["h_product_pk"]}','{p["h_restaurant_pk"]}','{load_dt}','{load_src}')
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;"""
                    )


    def insert_dds_sat(self,
                       load_dt: datetime,
                       load_src: str,
                       user: dict,
                       order: dict,
                       restaurant: dict,
                       products: dict) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:  
                cur.execute(
                    f"""insert into dds.s_user_names
                              (        h_user_pk,            userlogin,            username,      load_dt,    load_src  )
                        values('{user["h_user_pk"]}','{user["userlogin"]}','{user["username"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_user_pk) DO NOTHING;"""
                )
                
                cur.execute(
                    f"""insert into dds.s_order_cost
                              (         h_order_pk,            cost,           payment,     load_dt,    load_src  )
                        values('{order["h_order_pk"]}',{order["cost"]},{order["payment"]},'{load_dt}','{load_src}')
                        ON CONFLICT (h_order_pk) DO NOTHING;"""
                )
                    
                cur.execute(
                    f"""insert into dds.s_order_status
                              (         h_order_pk,             status,      load_dt,    load_src  )
                        values('{order["h_order_pk"]}','{order["status"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_order_pk) DO NOTHING;"""
                )
                  
                cur.execute(
                    f"""insert into dds.s_restaurant_names
                              (              h_restaurant_pk,                  name,                 load_dt,    load_src  )
                        values('{restaurant["h_restaurant_pk"]}','{restaurant["restaurant_name"]}','{load_dt}','{load_src}')
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;"""
                )
                   
                for p in products: # грузим список
                    cur.execute(
                        f"""insert into dds.s_product_names
                                (       h_product_pk,         name,              load_dt,    load_src  )
                            values('{p["h_product_pk"]}','{p["product_name"]}','{load_dt}','{load_src}')
                            ON CONFLICT (h_product_pk) DO NOTHING;"""
                    )
                  
                



