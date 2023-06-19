--INSERT INTO table (column_1, column_2)
--VALUES('value_1','value_2') 
--ON CONFLICT (column_1) 
--DO NOTHING; 
--
--
--select * from cdm.test
--CREATE TABLE cdm.test
--(
--	test_1 int NOT NULL PRIMARY KEY,
--	test_2 int NOT null
--)
--
--
--insert into cdm.test(test_1, test_2)
--values(1,2)
--ON CONFLICT (test_1) 
--DO update SET; 




DROP TABLE IF EXISTS cdm.user_product_counters;

CREATE TABLE cdm.user_product_counters
(
	id				serial 	NOT NULL 	PRIMARY KEY,
	user_id 		UUID	NOT NULL,
	product_id 		UUID	NOT NULL,
	product_name 	varchar NOT NULL,
	order_cnt 		int 	NOT NULL	check(order_cnt > 0),
	UNIQUE (user_id, product_id) 
);



DROP TABLE IF EXISTS cdm.user_category_counters;

CREATE TABLE cdm.user_category_counters
(
	id				serial 	NOT NULL 	PRIMARY KEY,
	user_id 		UUID	NOT NULL,
	category_id 	UUID	NOT NULL,
	category_name 	varchar NOT NULL,
	order_cnt 		int 	NOT NULL	check(order_cnt > 0),
	UNIQUE (user_id, category_id) 
);

--
--
--DROP TABLE IF EXISTS stg.order_events;
--
--CREATE TABLE stg.order_events
--(
--	id				serial 		NOT NULL 	PRIMARY KEY,
--	object_id 		integer		NOT null,
--	payload 		json		NOT null,
--	object_type 	varchar		NOT null,
--	sent_dttm 		timestamp	NOT null,
--	UNIQUE (object_id) 
--);
--





--Рестораны, пользователи, меню
--h_user — пользователь,
--h_product — продукт,
--h_category — категория продукта,
--h_restaurant — ресторан,
--h_order — заказ.



DROP TABLE IF EXISTS dds.s_user_names;
DROP TABLE IF EXISTS dds.s_product_names;
DROP TABLE IF EXISTS dds.s_restaurant_names;
DROP TABLE IF EXISTS dds.s_order_cost;
DROP TABLE IF EXISTS dds.s_order_status;

DROP TABLE IF EXISTS dds.l_order_product;
DROP TABLE IF EXISTS dds.l_product_restaurant;
DROP TABLE IF EXISTS dds.l_product_category;
DROP TABLE IF EXISTS dds.l_order_user;

DROP TABLE IF EXISTS dds.h_user;
DROP TABLE IF EXISTS dds.h_product;
DROP TABLE IF EXISTS dds.h_category;
DROP TABLE IF EXISTS dds.h_restaurant;
DROP TABLE IF EXISTS dds.h_order;




CREATE TABLE dds.h_user
(
	h_user_pk		UUID 		NOT NULL 	PRIMARY KEY,
	user_id			VARCHAR		NOT null,
	load_dt 		timestamp	NOT null,
	load_src		varchar		NOT null
);

CREATE TABLE dds.h_product
(
	h_product_pk	UUID 		NOT NULL 	PRIMARY KEY,
	product_id		VARCHAR		NOT null,
	load_dt 		timestamp	NOT null,
	load_src		varchar		NOT null
);

CREATE TABLE dds.h_category
(
	h_category_pk	UUID 		NOT NULL 	PRIMARY KEY,
	category_name	VARCHAR		NOT null,
	load_dt 		timestamp	NOT null,
	load_src		varchar		NOT null
);

CREATE TABLE dds.h_restaurant
(
	h_restaurant_pk	UUID 		NOT NULL 	PRIMARY KEY,
	restaurant_id	VARCHAR		NOT null,
	load_dt 		timestamp	NOT null,
	load_src		varchar		NOT null
);
 
CREATE TABLE dds.h_order 
(
	h_order_pk		UUID 		NOT NULL 	PRIMARY KEY,
	order_id		integer		NOT null,
	order_dt 		timestamp	NOT null,
	load_dt 		timestamp	NOT null,
	load_src		varchar		NOT null
);



CREATE TABLE dds.l_order_product
(
	hk_order_product_pk UUID 		NOT NULL 	PRIMARY KEY,
	h_order_pk			UUID		NOT null 	references dds.h_order(h_order_pk),
	h_product_pk		UUID		NOT null	references dds.h_product(h_product_pk),
	load_dt 			timestamp 	NOT null,
	load_src			varchar		NOT null
);

CREATE TABLE dds.l_product_restaurant
(
	hk_product_restaurant_pk 	UUID 		NOT NULL 	PRIMARY KEY,
	h_product_pk				UUID		NOT null 	references dds.h_product(h_product_pk),
	h_restaurant_pk				UUID		NOT null	references dds.h_restaurant(h_restaurant_pk),
	load_dt 					timestamp 	NOT null,
	load_src					varchar		NOT null
);	

CREATE TABLE dds.l_product_category
(
	hk_product_category_pk UUID 	NOT NULL 	PRIMARY KEY,
	h_product_pk		UUID		NOT null 	references dds.h_product(h_product_pk),
	h_category_pk		UUID		NOT null	references dds.h_category(h_category_pk),
	load_dt 			timestamp 	NOT null,
	load_src			varchar		NOT null
);	

CREATE TABLE dds.l_order_user
(
	hk_order_user_pk 	UUID 		NOT NULL 	PRIMARY KEY,
	h_order_pk			UUID		NOT null 	references dds.h_order(h_order_pk),
	h_user_pk			UUID		NOT null	references dds.h_user(h_user_pk),
	load_dt 			timestamp 	NOT null,
	load_src			varchar		NOT null
);



CREATE TABLE dds.s_user_names
(
	h_user_pk				UUID		NOT null	references dds.h_user(h_user_pk),
	load_dt 				timestamp 	NOT null,
	username				varchar		NOT null,
	userlogin				varchar		NOT null,
	load_src				varchar		NOT null,
	PRIMARY KEY(h_user_pk)
);

CREATE TABLE dds.s_product_names
(
	h_product_pk				UUID		NOT null	references dds.h_product(h_product_pk),
	name						varchar		NOT null,
	load_dt 					timestamp 	NOT null,
	load_src					varchar		NOT null,
	PRIMARY KEY(h_product_pk)
);

CREATE TABLE dds.s_restaurant_names
(
	h_restaurant_pk					UUID		NOT null	references dds.h_restaurant(h_restaurant_pk),
	name							varchar		NOT null,
	load_dt 						timestamp 	NOT null,
	load_src						varchar		NOT null,
	PRIMARY KEY(h_restaurant_pk)
);

CREATE TABLE dds.s_order_cost
(
	h_order_pk				UUID			NOT null	references dds.h_order(h_order_pk),
	cost					decimal(19, 5) 	NOT null,
	payment					decimal(19, 5) 	NOT null,
	load_dt 				timestamp 		NOT null,
	load_src				varchar			NOT null,
	PRIMARY KEY(h_order_pk)
);

CREATE TABLE dds.s_order_status
(
	h_order_pk					UUID			NOT null	references dds.h_order(h_order_pk),
	status						VARCHAR 		NOT null,
	load_dt 					timestamp 		NOT null,
	load_src					varchar			NOT null,
	PRIMARY KEY(h_order_pk)
);



