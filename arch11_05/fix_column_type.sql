-- Изменение типа колонки Unnamed: 14 с numeric на text
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 14" TYPE text USING "Unnamed: 14"::text; 