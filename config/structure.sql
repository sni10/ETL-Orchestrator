create database db2;

create schema walmart_usa;

create table db2.walmart_usa.source_types
(
    id   serial primary key,
    name text not null unique
);

alter table db2.walmart_usa.source_types
    owner to airflow;

create table db2.walmart_usa.suppliers
(
    supplier_id      integer,
    name             text,
    active           boolean,
    type_id          integer,
    source           text,
    column_map_rules jsonb,
    created_at       timestamp,
    updated_at       timestamp,
    range            varchar(255),
    version          bigint
);

alter table db2.walmart_usa.suppliers
    owner to airflow;

create index supplier_id
    on db2.walmart_usa.suppliers (supplier_id);

create index active
    on db2.walmart_usa.suppliers (active);

create index type_id
    on db2.walmart_usa.suppliers (type_id);




create table db2.walmart_usa.products_output
(
    version        bigint,
    upc            varchar(13),
    price          numeric(10, 2) default NULL::numeric,
    asin           varchar,
    qty            integer,
    supplier_id    integer,
    is_deleted     timestamp,
    created_at     timestamp      default CURRENT_TIMESTAMP,
    updated_at     timestamp,
    parentasin     varchar,
    version_asin   bigint,
    brand          varchar,
    name           varchar,
    lang           varchar,
    marketplace_id varchar
);

alter table db2.walmart_usa.products_output
    owner to airflow;

create index idx_upc
    on db2.walmart_usa.products_output (upc);

create index idx_is_deleted
    on db2.walmart_usa.products_output (is_deleted);

create unique index idx_upc_supplier_id
    on db2.walmart_usa.products_output (upc, supplier_id);


create table db2.walmart_usa.search_requests
(
    id         serial
        primary key,
    domain_id  integer                    not null,
    brand      varchar(255) default NULL::character varying,
    status     varchar(255) default NULL::character varying,
    time_from  date,
    time_to    date,
    step_back  integer,
    is_active  boolean      default false not null,
    version    bigint,
    created_at timestamp,
    updated_at timestamp
);

alter table db2.walmart_usa.search_requests
    owner to airflow;


create table db2.walmart_usa.find_products
(
    search_request_id integer,
    domain_id         integer,
    brand             varchar(255) default NULL::character varying,
    asin              varchar(20)  default NULL::character varying
        unique,
    title             text,
    upc_list          text[],
    ean_list          text[],
    json_data         jsonb,
    created_at        timestamp,
    time_from         date,
    time_to           date,
    version           bigint,
    status            varchar,
    is_deleted        timestamp,
    updated_at        timestamp,
    constraint find_products_unique
        unique (search_request_id, domain_id, brand, asin)
);

alter table db2.walmart_usa.find_products
    owner to airflow;

insert into db2.walmart_usa.source_types (id, name)
values  (1, 'google_sheet'),
        (2, 'url_csv'),
        (4, 'url_xlsx'),
        (3, 'google_drive');

insert into db2.walmart_usa.suppliers (supplier_id, name, active, type_id, source, column_map_rules, created_at, updated_at, range, version)
values  (19, 'MJG Trading (Drive)', true, 1, '1_HCqGhswe0Wc3vr-6rQRwjGDWK-6q2r5IaFFDSrpIa8', '{"qty": ["Quantity on hand units", "min"], "upc": "UPC", "price": ["Item price", "max"], "status": ["Sublocation", "addArray"]}', '2024-11-24 17:02:52.128086', '2025-02-19 10:35:09.309620', 'A1:S', 1739961309344629),
        (14, 'B2b frames', false, 4, 'https://cabinet.b2bframes.com/media/userfiles/product_list_wixez.xlsx', '{"qty": "Stock", "upc": "Barcode", "price": "Price"}', '2024-11-24 17:02:52.128086', '2025-01-28 10:42:50.314407', null, 1738060970282892),
        (18, 'Immediate Apparel', false, 3, '1bvn7cBnvnfQM_-yhIdPOtn2RvXG6wzXt', '{"qty": "OTS", "upc": "UPC #", "price": "Sale Price"}', '2024-11-24 17:02:52.128086', '2025-01-28 10:42:50.322252', null, 1738060970282788),
        (16, 'Notions Marketing Corporation', false, 1, '1duf8Yfc7p5hoXtdOz_6USO0tucEfaqDgIMb1lJ7-uxc', '{"qty": "AVAILABLE", "upc": "UPC", "price": "Retail Price"}', '2024-11-24 17:02:52.128086', '2025-01-28 10:42:50.318020', 'A1:M', 1738060970169142),
        (9, 'NYWD', false, 4, 'https://www.dropbox.com/scl/fi/ircfuhjco8tyxkuwticyy/B2B_Data_Feed_3971.xlsx?rlkey=z76c2d15aaetug36xmg1iho4r&dl=1', '{"qty": "Quantity", "upc": "UPC", "price": "SalePrice"}', '2024-11-24 16:36:10.880622', '2025-01-28 10:42:50.295195', null, 1738060970283188),
        (15, 'MIRA ENTERPRISES INC', false, 1, '1TQ7EQFuTGtRbZFOLvrexi_4xGwP-FGxRv0G8j1JFnJs', '{"qty": "STOCK", "upc": "BARCODE", "price": "PRICE"}', '2024-11-24 17:02:52.128086', '2025-01-28 10:42:50.316128', 'A1:D', 1738060970282547),
        (17, 'CTW Home', false, 3, '1f2QUsPSz53KXB-fnMk4sUDmSiQqZJpyw', '{"qty": "QTY AVAILABLE", "upc": "UD F UPC", "price": "Std Price"}', '2024-11-24 17:02:52.128086', '2025-01-28 10:42:50.320413', null, 1738060970283301),
        (10, 'MJG Trading', false, 1, '1GnLEboeoTsG9qUOEpJx-dSjFrFxaPHHiehbKmcKmJaM', '{"qty": "Quantity on hand units", "upc": "UPC", "price": "Item price"}', '2024-11-24 16:36:10.880622', '2025-01-28 10:42:50.308773', 'A1:O', 1738060970283505),
        (7, 'Warehouse USA', false, 1, '1QrkgQOV7p9M1fTIjl-DVZReSlLi8GGiYa13FqrxJafA', '{"qty": "quantity", "upc": "UPC", "price": "price"}', '2024-11-24 16:36:10.880622', '2025-01-28 10:42:50.293212', 'A1:D', 1738060970283356),
        (13, 'Accuratime', false, 2, 'http://feeds.accuratime.com/feeds/mcw.csv', '{"qty": "Qty", "upc": "UPC", "price": "Wholesale"}', '2024-11-24 16:36:10.880622', '2025-02-14 10:48:38.440263', null, 1739530118453687),
        (5, 'Zager Watch', false, 2, 'https://zagerwatch.com/product-csv/key:db27ab2f39363b813f1886da0bc67844', '{"qty": "Stock Qty", "upc": "UPC Code", "price": "price"}', '2024-11-24 16:36:10.880622', '2025-02-14 10:48:38.440263', null, 1739530118453687),
        (6, 'Top Dawg', false, null, 'https://topdawg.com/api/TDApi/ResellerProduct/list', '{}', '2024-11-24 16:36:10.880622', '2024-11-24 16:36:10.880622', null, null),
        (12, 'Caseys', false, 1, '1bmgUt8xpniN1wSy5eal9gxnxZbq1RPc0zCAjd8RcD24', '{"qty": "Stock Level", "upc": "Product UPC/EAN", "price": "Price"}', '2024-11-24 16:36:10.880622', '2025-02-14 10:48:38.440263', 'A1:E', 1739530118453687),
        (8, 'Burch Fishing Tackle', false, 2, 'https://www.dropbox.com/s/j78jr9llqpury5a/Burch%20Tackle%20Current%20Inventory.csv?dl=1', '{}', '2024-11-24 16:36:10.880622', '2024-11-24 16:36:10.880622', null, null),
        (11, 'B2B Sportswear', false, 1, '1sByR5Yd0aCbX74zhC_pn2X_nqTsbs3-OqkcmYds8eE8', '{}', '2024-11-24 16:36:10.880622', '2024-12-24 13:04:49.792637', null, null),
        (2, 'Amazon CA', false, null, null, '{}', '2024-11-24 17:02:52.128086', '2024-11-24 17:02:52.128086', null, null),
        (4, 'Walmart CA', false, null, null, '{}', '2024-11-24 17:02:52.128086', '2024-11-24 17:02:52.128086', null, null),
        (1, 'Amazon USA', false, null, null, '{}', '2024-11-24 17:02:52.128086', '2024-11-24 17:02:52.128086', null, null),
        (3, 'Walmart USA', false, null, null, '{}', '2024-11-24 17:02:52.128086', '2024-11-24 17:02:52.128086', null, null);


insert into walmart_usa.search_requests (id, domain_id, brand, status, time_from, time_to, step_back, is_active, version, created_at, updated_at)
values  (5, 3, 'hot wheels', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (6, 3, 'Nike', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (3, 3, 'star wars', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (7, 3, 'Adidas', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (4, 3, 'lego', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (2, 3, 'loungefly', null, '2025-01-01', null, 24, false, 1739903564652005, '2025-02-07 08:44:58.000000', '2025-02-18 18:32:44.649770'),
        (1, 3, 'funko', null, '2025-01-01', null, 24, true, 1739955783156794, '2025-02-07 08:44:58.000000', '2025-02-19 09:03:03.154428');