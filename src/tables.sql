DROP TABLE IF EXISTS stg.couriers CASCADE;
DROP TABLE IF EXISTS stg.restaurants CASCADE;
DROP TABLE IF EXISTS stg.deliveries CASCADE;
DROP TABLE IF EXISTS dds.fct_courier_deliveries CASCADE;
DROP TABLE IF EXISTS dds.dds_restaurants CASCADE;
DROP TABLE IF EXISTS dds.dds_couriers CASCADE;
DROP TABLE IF EXISTS dds.dds_deliveries CASCADE;
DROP TABLE IF EXISTS cdm.dm_courier_ledger CASCADE;

CREATE TABLE stg.couriers (
	id serial4 NOT NULL,
	courier_info text NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT couriers_pk PRIMARY KEY (id)
);

CREATE TABLE stg.restaurants (
	id serial4 NOT NULL,
	restaurant_info text NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT restaurants_pk PRIMARY KEY (id)
);

CREATE TABLE stg.deliveries (
	id serial4 NOT NULL,
	delivery_info text NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT deliveries_pk PRIMARY KEY (id)
);

CREATE TABLE dds.dds_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT dds_restaurants_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX dds_restaurants_restaurant_id_idx ON dds.dds_restaurants (restaurant_id);

CREATE TABLE dds.dds_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT dds_couriers_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX dds_couriers_courier_id_idx ON dds.dds_couriers (courier_id);

CREATE TABLE dds.dds_deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	restaurant_id varchar,
	address text NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT dds_deliveries_pk PRIMARY KEY (id)
);

CREATE TABLE dds.fct_courier_deliveries (
	id serial4 NOT NULL,
	id_courier int4 NOT NULL,
	id_restaurant int4,
	id_delivery int4 NOT NULL,
	delivery_sum numeric(14, 2) NOT NULL,
	delivery_tip_sum numeric(14, 2) NOT NULL,
	delivery_rate int4 NOT NULL,
	fct_ts timestamp NOT NULL,
	CONSTRAINT fct_courier_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT fct_courier_deliveries_fk_couriers FOREIGN KEY (id_courier) REFERENCES dds.dds_couriers(id)
	-- CONSTRAINT fct_courier_deliveries_fk_restaurants FOREIGN KEY (id_restaurant) REFERENCES dds.dds_restaurants(id)
);

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(14, 2) NOT NULL,
	orders_processing_fee numeric(14, 2) NOT NULL,
	courier_orders_sum numeric(14, 2) NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL,
	courier_reward_sum numeric(14, 2) NOT NULL,
	CONSTRAINT cdm_dm_courier_ledger_pk PRIMARY KEY (id)
	);
	