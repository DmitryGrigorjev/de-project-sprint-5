TRUNCATE TABLE stg.couriers restart IDENTITY cascade;
TRUNCATE TABLE stg.restaurants restart IDENTITY cascade;
TRUNCATE TABLE stg.deliveries restart IDENTITY cascade;
TRUNCATE TABLE dds.fct_courier_deliveries restart IDENTITY cascade;
TRUNCATE TABLE dds.dds_restaurants restart IDENTITY cascade;
TRUNCATE TABLE dds.dds_couriers restart IDENTITY cascade;
TRUNCATE TABLE dds.dds_deliveries restart IDENTITY cascade;
TRUNCATE TABLE cdm.dm_courier_ledger restart IDENTITY cascade;

-- couriers
INSERT INTO stg.couriers
(courier_info, update_ts)
VALUES('[{"id": "c80984a9-1028-4305-a556-061464cd46fb","name":"Bart Simpson"}]', '2022-10-14 13:43:03.653');
INSERT INTO stg.couriers
(courier_info, update_ts)
VALUES('[{"id": "f6327d31-1e1b-4e73-980a-f13e91aa30c5","name":"Lisa Simpson"}]', '2022-10-14 13:43:16.481');
INSERT INTO stg.couriers
(courier_info, update_ts)
VALUES('[{"id": "239d3dcd-4c73-42f2-a3c1-1c87112db94f","name":"Dana Scully"}]', '2022-10-14 13:43:36.231');
INSERT INTO stg.couriers
(courier_info, update_ts)
VALUES('[{"id": "74db9c90-31d4-48f4-bff9-fce9231b6c28","name":"Homer Simpson"}]', '2022-10-14 13:43:49.390');
INSERT INTO stg.couriers
(courier_info, update_ts)
VALUES('[{"id": "ae2fa120-ea04-41e3-bcfc-f227bc635264","name":"Fox Mulder"}]', '2022-10-14 13:44:02.532');

-- restaurants
INSERT INTO stg.restaurants
(restaurant_info, update_ts)
VALUES('[{"id": "d261acb7-ac86-4f86-ba15-796cc3ef380c","name": "Moes Tavern"}]', '2022-10-14 13:50:18.059');
INSERT INTO stg.restaurants
(restaurant_info, update_ts)
VALUES('[{"id": "aee3a8a4-e1aa-4aec-bce3-5a6acabe29be","name": "Nag and Wease"}]', '2022-10-14 13:50:18.063');
INSERT INTO stg.restaurants
(restaurant_info, update_ts)
VALUES('[{"id": "8226fa8c-75b0-471a-b5d7-c92cab381366","name": "Tokyo Ro"}]', '2022-10-14 13:50:18.067');

-- deliveries
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "80f5e5f4-9b79-4c62-bd74-318147003492","order_id": "4a199bef-9d24-4ea5-858b-be4a40c2b7ad","delivery_id": "46839564-74b5-42cd-b04c-e566a12510c5","courier_id": "c80984a9-1028-4305-a556-061464cd46fb","restaurant_id": "d261acb7-ac86-4f86-ba15-796cc3ef380c","address": "712 Red Bark Lane, Henderson, Clark County, Nevada 89011", "delivery_ts": "2022-09-14 14:10:18.059", "rate": "5","sum": "132", "tip_sum": "10"]}','2022-09-14 14:15:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "8b99d71f-b649-4720-a307-eb2ee148b121","order_id": "bf96ae4d-bdc6-48be-aa4b-2ad3e9236b1a","delivery_id": "8096f901-b49d-4fb4-b1fe-9c773be07445","courier_id": "c80984a9-1028-4305-a556-061464cd46fb","restaurant_id": "8226fa8c-75b0-471a-b5d7-c92cab381366","address": "742 Evergreen Terrace, Springfield, Nevada 89000","delivery_ts": "2022-09-14 14:20:18.059","rate": "5","sum": "150","tip_sum": "20"]}','2022-09-14 14:25:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "b14700ee-2443-419a-b75b-175092a84576","order_id": "487ae39b-085b-451c-9beb-4a004842645f","delivery_id": "d8a5947d-0b5b-4504-8d62-0bd51d9854c1","courier_id": "c80984a9-1028-4305-a556-061464cd46fb","restaurant_id": "d261acb7-ac86-4f86-ba15-796cc3ef380c","address": "712 Red Bark Lane, Henderson, Nevada 89011","delivery_ts": "2022-09-14 14:30:18.059","rate": "4","sum": "100","tip_sum": "10"]}','2022-09-14 14:35:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "e6e04bf6-4a16-4323-a41d-397c1766a577","order_id": "25f45abc-d6fd-48af-bbda-f727b98061d4","delivery_id": "e00127a4-0add-467d-8f08-6342f611b333","courier_id": "74db9c90-31d4-48f4-bff9-fce9231b6c28","restaurant_id": "d261acb7-ac86-4f86-ba15-796cc3ef380c","address": "712 Red Bark Lane, Henderson, Nevada 89011","delivery_ts": "2022-09-14 14:40:18.059","rate": "4","sum": "1200","tip_sum": "50"]}','2022-09-14 14:45:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "a548ea11-a9e7-4d7f-8d25-58523e16dd57","order_id": "287beb7c-e684-470c-b2e5-910c95f33d7e","delivery_id": "e00127a4-0add-467d-8f08-6342f611b333","courier_id": "f6327d31-1e1b-4e73-980a-f13e91aa30c5","restaurant_id": "aee3a8a4-e1aa-4aec-bce3-5a6acabe29be","address": "742 Evergreen Terrace, Springfield, Nevada 89000","delivery_ts": "2022-09-14 14:50:18.059","rate": "4","sum": "250","tip_sum": "5"]}','2022-09-14 14:55:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "f65d25a8-f334-45b8-b9e6-7a73b6127c29","order_id": "76e54117-c277-4ba1-83a0-c020289f83f8","delivery_id": "307bce5e-97ad-404c-850d-22ee7cd2143d","courier_id": "239d3dcd-4c73-42f2-a3c1-1c87112db94f","restaurant_id": "d261acb7-ac86-4f86-ba15-796cc3ef380c","address": "742 Evergreen Terrace, Springfield, Nevada 89000","delivery_ts": "2022-09-14 15:50:18.059","rate": "1","sum": "100","tip_sum": "0"]}','2022-09-15 14:55:18.067');
INSERT INTO stg.deliveries
(delivery_info, update_ts)
VALUES('[{"id": "95d581bb-39d0-4d20-a8e6-b57844b5b333","order_id": "c1dadb86-8b5f-4d01-8f79-0c9bab71c718","delivery_id": "90213518-c8ed-4d11-8184-15b3603f8069","courier_id": "ae2fa120-ea04-41e3-bcfc-f227bc635264","restaurant_id": "aee3a8a4-e1aa-4aec-bce3-5a6acabe29be","address": "742 Evergreen Terrace, Springfield, Nevada 89000","delivery_ts": "2022-09-14 16:50:18.059","rate": "3","sum": "5","tip_sum": "0"]}','2022-09-14 16:55:18.067');