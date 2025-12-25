-- Создание пользователя
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'coreuser') THEN
        CREATE USER coreuser WITH PASSWORD 'corepass';
    END IF;
END
$$;

-- Выдаем права
GRANT ALL PRIVILEGES ON DATABASE coredb TO coreuser;

-- Меняем владельца базы
ALTER DATABASE coredb OWNER TO coreuser;

-- Создаем таблицы (уже в контексте базы coredb)
-- Создание таблиц в правильном порядке для избежания циклических зависимостей
CREATE TABLE IF NOT EXISTS "ore_deposits" (
  "id" SERIAL NOT NULL UNIQUE,
  "name" VARCHAR(255) NOT NULL UNIQUE,
  "status" VARCHAR(255) NOT NULL,
  "discovery_year" INTEGER NOT NULL,
  "latitude" NUMERIC NOT NULL,
  "longitude" NUMERIC NOT NULL,
  "has_railroad" BOOLEAN NOT NULL,
  "has_power_supply" BOOLEAN NOT NULL,
  "nearby_settlement" VARCHAR(255),
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "minerals" (
  "id" SERIAL NOT NULL UNIQUE,
  "name" VARCHAR(255) NOT NULL UNIQUE,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "buyers_companies" (
  "id" SERIAL NOT NULL UNIQUE,
  "name" VARCHAR(255) NOT NULL,
  "license_number" INTEGER NOT NULL UNIQUE,
  "contact_name" VARCHAR(255),
  "contact_phone" VARCHAR(255),
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "deposit_owner" (
  "id" SERIAL NOT NULL UNIQUE,
  "ore_deposit_id" INTEGER NOT NULL,
  "name" VARCHAR(255) NOT NULL,
  "acquisition_year" INTEGER NOT NULL,
  "expiration_year" INTEGER NOT NULL,
  "contact_name" VARCHAR(255),
  "contact_phone" VARCHAR(255),
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "reserves" (
  "id" SERIAL NOT NULL UNIQUE,
  "ore_deposit_id" INTEGER NOT NULL,
  "mineral_id" INTEGER NOT NULL,
  "absolute_volume" NUMERIC NOT NULL,
  "is_confirmed" BOOLEAN NOT NULL,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "miners" (
  "id" SERIAL NOT NULL UNIQUE,
  "name" VARCHAR(255),
  "phone" VARCHAR(255),
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "team_names" (
  "id" SERIAL NOT NULL UNIQUE,
  "name" VARCHAR(255) NOT NULL UNIQUE,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "teams" (
  "id" SERIAL NOT NULL UNIQUE,
  "name_id" INTEGER NOT NULL,
  "miners_id" INTEGER NOT NULL,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "mining_teams" (
  "id" SERIAL NOT NULL UNIQUE,
  "team_id" INTEGER NOT NULL,
  "foreman_name" VARCHAR(255),
  "foreman_phone" VARCHAR(255),
  "is_active" BOOLEAN NOT NULL,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "work_shifts" (
  "id" SERIAL NOT NULL UNIQUE,
  "mining_team_id" INTEGER NOT NULL,
  "ore_deposit_id" INTEGER NOT NULL,
  "shift_date" DATE NOT NULL,
  "start_time" TIME NOT NULL,
  "end_time" TIME NOT NULL,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "shift_production" (
  "id" SERIAL NOT NULL UNIQUE,
  "shift_id" INTEGER NOT NULL,
  "mineral_id" INTEGER NOT NULL,
  "tons_of_ore" NUMERIC NOT NULL,
  "equipment_damaged" BOOLEAN NOT NULL,
  "notes" TEXT,
  PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "sales_to_companies" (
  "id" SERIAL NOT NULL UNIQUE,
  "owner_id" INTEGER NOT NULL,
  "buyer_id" INTEGER NOT NULL,
  "mineral_id" INTEGER NOT NULL,
  "sale_date" DATE NOT NULL,
  "sold_tons" NUMERIC NOT NULL,
  "sale_price_per_ton" NUMERIC NOT NULL,
  PRIMARY KEY("id")
);

-- Добавление внешних ключей
ALTER TABLE "deposit_owner"
ADD FOREIGN KEY("ore_deposit_id") REFERENCES "ore_deposits"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "reserves"
ADD FOREIGN KEY("ore_deposit_id") REFERENCES "ore_deposits"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "reserves"
ADD FOREIGN KEY("mineral_id") REFERENCES "minerals"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "teams"
ADD FOREIGN KEY("name_id") REFERENCES "team_names"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "teams"
ADD FOREIGN KEY("miners_id") REFERENCES "miners"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "mining_teams"
ADD FOREIGN KEY("team_id") REFERENCES "teams"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "work_shifts"
ADD FOREIGN KEY("mining_team_id") REFERENCES "mining_teams"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "work_shifts"
ADD FOREIGN KEY("ore_deposit_id") REFERENCES "ore_deposits"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "shift_production"
ADD FOREIGN KEY("shift_id") REFERENCES "work_shifts"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "shift_production"
ADD FOREIGN KEY("mineral_id") REFERENCES "minerals"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "sales_to_companies"
ADD FOREIGN KEY("owner_id") REFERENCES "deposit_owner"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "sales_to_companies"
ADD FOREIGN KEY("buyer_id") REFERENCES "buyers_companies"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

ALTER TABLE "sales_to_companies"
ADD FOREIGN KEY("mineral_id") REFERENCES "minerals"("id")
ON UPDATE NO ACTION ON DELETE NO ACTION;

-- Вставка данных о минералах
INSERT INTO minerals (name) VALUES
('Золото'),('Медь'),('Железо'),('Алмазы'),('Уран'),('Серебро'),('Платина'),('Никель'),
('Свинец'),('Цинк'),('Олово'),('Вольфрам'),('Молибден'),('Кобальт'),('Хром'),('Марганец'),
('Титан'),('Ванадий'),('Бериллий'),('Литий')
ON CONFLICT (name) DO NOTHING;

-- Функции для генерации тестовых данных
CREATE OR REPLACE FUNCTION gen_first_name() RETURNS TEXT AS $$
SELECT (array['Александр','Алексей','Андрей','Антон','Артем','Борис','Вадим','Валентин','Валерий','Виктор','Владимир','Дмитрий','Евгений','Иван','Игорь','Кирилл','Константин','Максим','Михаил','Никита','Николай','Олег','Павел','Роман','Сергей','Юрий'])[floor(random()*26+1)];
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION gen_last_name() RETURNS TEXT AS $$
SELECT (array['Иванов','Петров','Сидоров','Смирнов','Кузнецов','Попов','Васильев','Михайлов','Новиков','Федоров','Морозов','Волков','Алексеев','Лебедев','Семенов','Егоров','Павлов','Козлов','Степанов','Николаев','Орлов','Андреев','Макаров','Никитин','Захаров'])[floor(random()*25+1)];
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION gen_company() RETURNS TEXT AS $$
SELECT (array['ООО','ЗАО','АО','ПАО'])[floor(random()*4+1)] || ' ' || 
       (array['Горный','Рудный','Металл','Урал','Сибирь','Базальт','Гранит','Шахтный','Карьер','Геолог'])[floor(random()*10+1)] || 
       (array['Холдинг','Групп','Компания','Трест','Инвест','Пром'])[floor(random()*6+1)];
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION gen_deposit_name() RETURNS TEXT AS $$
SELECT (array['Северный','Южный','Западный','Восточный','Центральный','Новый','Старый'])[floor(random()*7+1)] || ' ' ||
       (array['рудник','прииск','карьер','шахта','месторождение'])[floor(random()*5+1)];
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION gen_phone() RETURNS TEXT AS $$
SELECT '+7' || floor(random() * 9000000000 + 1000000000)::bigint;
$$ LANGUAGE SQL;

-- Вставка тестовых данных
INSERT INTO ore_deposits (name, status, discovery_year, latitude, longitude, has_railroad, has_power_supply, nearby_settlement)
SELECT 
    gen_deposit_name() || ' ' || gs,
    (array['разрабатывается','консервация','планируется','закрыто'])[floor(random()*4+1)],
    floor(random() * (2023-1950+1) + 1950)::int,
    45 + random() * 40,
    30 + random() * 100,
    random() > 0.3,
    random() > 0.2,
    'Поселок ' || (array['Горный','Рудный','Шахтерский','Северный','Южный'])[floor(random()*5+1)]
FROM generate_series(1, 100) gs
ON CONFLICT (name) DO NOTHING;

INSERT INTO buyers_companies (name, license_number, contact_name, contact_phone)
SELECT 
    gen_company() || ' ' || gs,
    1000 + gs,
    gen_first_name() || ' ' || gen_last_name(),
    gen_phone()
FROM generate_series(1, 1000) gs
ON CONFLICT (license_number) DO NOTHING;

INSERT INTO miners (name, phone)
SELECT 
    gen_first_name() || ' ' || gen_last_name(),
    gen_phone()
FROM generate_series(1, 100) gs
ON CONFLICT DO NOTHING;

INSERT INTO team_names (name)
SELECT 
    'Бригада ' || 
    (array['Альфа','Бета','Гамма','Дельта','Эпсилон','Зета','Эта','Тета','Йота','Каппа',
           'Северная','Южная','Западная','Восточная','Горная','Рудная','Шахтная'])[gs]
FROM generate_series(1, 15) gs
ON CONFLICT DO NOTHING;

INSERT INTO deposit_owner (ore_deposit_id, name, acquisition_year, expiration_year, contact_name, contact_phone)
SELECT 
    CEIL(100 * random()),
    gen_company() || ' ' || gs,
    floor(random() * (2023-2000+1) + 2000)::int,
    floor(random() * (2050-2024+1) + 2024)::int,
    gen_first_name() || ' ' || gen_last_name(),
    gen_phone()
FROM generate_series(1, 100) gs
ON CONFLICT DO NOTHING;

INSERT INTO reserves (ore_deposit_id, mineral_id, absolute_volume, is_confirmed)
SELECT 
    CEIL(100 * random()),
    CEIL(20 * random()),
    random() * 1000000 + 10000,
    random() > 0.2
FROM generate_series(1, 100) gs
ON CONFLICT DO NOTHING;

-- Вставка данных в правильном порядке для избежания зависимостей
INSERT INTO teams (name_id, miners_id)
SELECT 
    tn.id as name_id,
    m.id as miners_id
FROM team_names tn
CROSS JOIN miners m
WHERE random() < 0.3 
ON CONFLICT DO NOTHING;

INSERT INTO mining_teams (team_id, foreman_name, foreman_phone, is_active)
SELECT 
    CEIL(15 * random()),
    gen_first_name() || ' ' || gen_last_name(),
    gen_phone(),
    random() > 0.1
FROM generate_series(1, 50) gs
ON CONFLICT DO NOTHING;

INSERT INTO work_shifts (mining_team_id, ore_deposit_id, shift_date, start_time, end_time)
SELECT 
    CEIL(50 * random()),
    (SELECT id FROM ore_deposits ORDER BY random() LIMIT 1),
    CURRENT_DATE - (random() * 365)::int,
    (TIME '06:00' + (random() * 7200) * INTERVAL '1 second'),
    (TIME '18:00' + (random() * 7200) * INTERVAL '1 second')
FROM generate_series(1, 100) gs
ON CONFLICT DO NOTHING;

INSERT INTO shift_production (shift_id, mineral_id, tons_of_ore, equipment_damaged, notes)
SELECT 
    CEIL(100 * random()),
    CEIL(20 * random()),
    random() * 500 + 50,
    random() > 0.9,
    CASE WHEN random() > 0.7 THEN 'Примечание: ' || (array['Плановые работы','Высокая производительность','Незначительные неисправности','Превышение плана','Обнаружены новые жилы'])[floor(random()*5+1)] ELSE NULL END
FROM generate_series(1, 100) gs
ON CONFLICT DO NOTHING;

INSERT INTO sales_to_companies (owner_id, buyer_id, mineral_id, sale_date, sold_tons, sale_price_per_ton)
SELECT 
    CEIL(100 * random()),
    CEIL(1000 * random()),
    CEIL(20 * random()),
    CURRENT_DATE - (random() * 730)::int,
    random() * 1000 + 100,
    random() * 50000 + 5000
FROM generate_series(1, 1000) gs
ON CONFLICT DO NOTHING;


CREATE OR REPLACE VIEW view_deposits_summary AS
SELECT 
    od.id,
    od.name AS deposit_name,
    od.status,
    od.discovery_year,
    od.latitude,
    od.longitude,
    CASE WHEN od.has_railroad THEN 'Да' ELSE 'Нет' END AS railroad_available,
    CASE WHEN od.has_power_supply THEN 'Да' ELSE 'Нет' END AS power_supply_available,
    od.nearby_settlement,
    dep_own.name AS owner_name,
    COALESCE(SUM(r.absolute_volume), 0) AS total_reserves_volume
FROM ore_deposits od
LEFT JOIN deposit_owner dep_own ON od.id = dep_own.ore_deposit_id
LEFT JOIN reserves r ON od.id = r.ore_deposit_id
GROUP BY od.id, od.name, od.status, od.discovery_year, od.latitude, od.longitude, 
         od.has_railroad, od.has_power_supply, od.nearby_settlement,
         dep_own.name
ORDER BY od.name;

CREATE OR REPLACE VIEW view_reserves_details AS
SELECT 
    r.id,
    od.name AS deposit_name,
    m.name AS mineral_name,
    r.absolute_volume,
    CASE WHEN r.is_confirmed THEN 'Подтверждено' ELSE 'Не подтверждено' END AS confirmation_status
FROM reserves r
JOIN ore_deposits od ON r.ore_deposit_id = od.id
JOIN minerals m ON r.mineral_id = m.id
ORDER BY od.name, m.name;

CREATE OR REPLACE VIEW view_sales_report AS
SELECT 
    s.id,
    od.name AS deposit_name,
    m.name AS mineral_name,
    bc.name AS buyer_company,
    bc.contact_name AS buyer_contact,
    s.sale_date,
    s.sold_tons,
    s.sale_price_per_ton,
    (s.sold_tons * s.sale_price_per_ton) AS total_amount,
    TO_CHAR(s.sold_tons * s.sale_price_per_ton, '999G999G999D99') || ' ₽' AS formatted_amount
FROM sales_to_companies s
JOIN deposit_owner dep_own ON s.owner_id = dep_own.id
JOIN ore_deposits od ON dep_own.ore_deposit_id = od.id
JOIN minerals m ON s.mineral_id = m.id
JOIN buyers_companies bc ON s.buyer_id = bc.id
ORDER BY s.sale_date DESC;

CREATE OR REPLACE VIEW view_production_daily AS
SELECT 
    sp.id,
    ws.shift_date,
    od.name AS deposit_name,
    mt.foreman_name,
    CONCAT(EXTRACT(HOUR FROM ws.start_time), ':', 
           LPAD(EXTRACT(MINUTE FROM ws.start_time)::text, 2, '0')) AS shift_start,
    CONCAT(EXTRACT(HOUR FROM ws.end_time), ':', 
           LPAD(EXTRACT(MINUTE FROM ws.end_time)::text, 2, '0')) AS shift_end,
    sp.tons_of_ore,
    CASE WHEN sp.equipment_damaged THEN 'Да' ELSE 'Нет' END AS equipment_damage,
    sp.notes
FROM shift_production sp
JOIN work_shifts ws ON sp.shift_id = ws.id
JOIN ore_deposits od ON ws.ore_deposit_id = od.id
JOIN mining_teams mt ON ws.mining_team_id = mt.id
WHERE ws.shift_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY ws.shift_date DESC;

CREATE OR REPLACE VIEW view_team_performance AS
SELECT 
    mt.id AS team_id,
    tn.name AS team_name,
    mt.foreman_name,
    mt.foreman_phone,
    COUNT(ws.id) AS total_shifts,
    SUM(sp.tons_of_ore) AS total_production,
    ROUND(AVG(sp.tons_of_ore), 2) AS avg_daily_production,
    SUM(CASE WHEN sp.equipment_damaged THEN 1 ELSE 0 END) AS damage_incidents
FROM mining_teams mt
JOIN teams t ON mt.team_id = t.id
JOIN team_names tn ON t.name_id = tn.id
LEFT JOIN work_shifts ws ON mt.id = ws.mining_team_id
LEFT JOIN shift_production sp ON ws.id = sp.shift_id
WHERE mt.is_active = true
GROUP BY mt.id, tn.name, mt.foreman_name, mt.foreman_phone
ORDER BY total_production DESC;

CREATE OR REPLACE VIEW view_mineral_prices AS
SELECT 
    m.id,
    m.name AS mineral_name,
    ROUND(AVG(s.sale_price_per_ton), 2) AS avg_price_per_ton,
    MIN(s.sale_price_per_ton) AS min_price_per_ton,
    MAX(s.sale_price_per_ton) AS max_price_per_ton,
    SUM(s.sold_tons) AS total_sold_volume,
    COUNT(DISTINCT bc.id) AS number_of_buyers
FROM minerals m
LEFT JOIN sales_to_companies s ON m.id = s.mineral_id
LEFT JOIN buyers_companies bc ON s.buyer_id = bc.id
GROUP BY m.id, m.name
ORDER BY m.name;