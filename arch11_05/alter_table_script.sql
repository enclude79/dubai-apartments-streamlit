-- Скрипт изменения типов данных в таблице bayut_properties

-- Колонка: Unnamed: 26, текущий тип: integer, рекомендуемый тип: VARCHAR(255)
-- Примеры значений: ['for-sale', 'for-sale', 'for-sale']
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 26" TYPE VARCHAR(255) USING "Unnamed: 26"::VARCHAR(255);

-- Колонка: Unnamed: 27, текущий тип: integer, рекомендуемый тип: NUMERIC
-- Примеры значений: []
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 27" TYPE NUMERIC USING "Unnamed: 27"::NUMERIC;

-- Колонка: Unnamed: 29, текущий тип: numeric, рекомендуемый тип: INTEGER
-- Примеры значений: [75, 75, 75]
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 29" TYPE INTEGER USING "Unnamed: 29"::INTEGER;

