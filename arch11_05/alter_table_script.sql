-- ������ ��������� ����� ������ � ������� bayut_properties

-- �������: Unnamed: 26, ������� ���: integer, ������������� ���: VARCHAR(255)
-- ������� ��������: ['for-sale', 'for-sale', 'for-sale']
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 26" TYPE VARCHAR(255) USING "Unnamed: 26"::VARCHAR(255);

-- �������: Unnamed: 27, ������� ���: integer, ������������� ���: NUMERIC
-- ������� ��������: []
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 27" TYPE NUMERIC USING "Unnamed: 27"::NUMERIC;

-- �������: Unnamed: 29, ������� ���: numeric, ������������� ���: INTEGER
-- ������� ��������: [75, 75, 75]
ALTER TABLE bayut_properties 
ALTER COLUMN "Unnamed: 29" TYPE INTEGER USING "Unnamed: 29"::INTEGER;

