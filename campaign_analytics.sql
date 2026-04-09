-- ============================================================
-- CAMPAIGN ANALYTICS SQL PROJECT
-- Aaliyah Orloff
-- Demonstrates data quality auditing, reconciliation queries,
-- and decision support analysis for marketing campaign data
-- ============================================================

-- ============================================================
-- SECTION 1: DATABASE SETUP
-- ============================================================

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS deliveries;

CREATE TABLE campaigns (
    campaign_id TEXT,
    campaign_name TEXT,
    start_date TEXT,
    end_date TEXT,
    budget REAL,
    channel TEXT
);

CREATE TABLE customers (
    customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    zip_code TEXT,
    segment TEXT,
    campaign_id TEXT,
    response_flag INTEGER,
    spend_amount REAL,
    record_date TEXT
);

CREATE TABLE deliveries (
    delivery_id INTEGER,
    customer_id INTEGER,
    campaign_id TEXT,
    delivery_date TEXT,
    delivery_status TEXT,
    record_count INTEGER
);

INSERT INTO campaigns VALUES
('C001','Winter Acquisition','2025-01-01','2025-03-31',50000.00,'Direct Mail'),
('C002','Spring Reactivation','2025-01-01','2025-06-30',35000.00,'Email'),
('C003','Q1 Retention','2025-01-01','2025-03-31',25000.00,'Digital');

INSERT INTO customers VALUES
(1001,'James','Holloway','j.holloway@email.com','72015','Active','C001',1,120.50,'2025-01-15'),
(1002,'Tanya','Reed','tanya.reed@email.com','71101','Prospect','C002',0,85.00,'2025-01-15'),
(1003,'Marcus','Sullivan','marcus.s@email.com','72034','Lapsed','C001',1,200.00,'2025-01-16'),
(1004,'Lisa','Chang','lisa.chang@email.com','90210','Active','C003',1,95.75,'2025-01-16'),
(1005,'Derek','Webb','derek.webb@email.com','71105','prospect','C002',0,110.00,'2025-01-17'),
(1006,'Sandra','Morton','not-an-email','ABCDE','Active','C001',1,88.50,'2025-01-17'),
(1007,'Paul','Brooks','paul.b@email.com','72015','Lapsed','C003',0,175.00,'2025-01-18'),
(1008,'Kevin','Nash','kevin.nash@email.com','72032','PROSPECT','C001',1,92.00,'2025-01-19'),
(1009,'Priya','Patel','priya.p@email.com','10001','Active','C003',1,5000.00,'2025-01-19'),
(1010,'Omar','Diaz','omar.d@email.com','75001','Lapsed','C002',0,105.00,'2025-01-20'),
(1002,'Tanya','Reed','tanya.reed@email.com','71101','Prospect','C002',0,85.00,'2025-01-15'),
(1011,'Beth','Ford','beth.ford@email.com','72019','Active','C001',1,130.00,'2025-01-20'),
(1012,'Chris','Lane','chris.lane@email.com','72011','Lapsed',NULL,0,78.00,'2025-01-21'),
(NULL,'Rachel','Green','rachel.g@email.com','72015','Prospect','C002',1,145.00,'2025-01-21');

INSERT INTO deliveries VALUES
(1,1001,'C001','2025-01-15','Delivered',4200),
(2,1002,'C002','2025-01-15','Delivered',3800),
(3,1003,'C001','2025-01-16','Delivered',4100),
(4,1004,'C003','2025-01-16','Delivered',2900),
(5,1005,'C002','2025-01-17','Failed',0),
(6,1006,'C001','2025-01-17','Delivered',4050),
(7,1007,'C003','2025-01-18','Delivered',2850),
(8,1008,'C001','2025-01-19','Delivered',4200),
(9,1009,'C003','2025-01-19','Delivered',2950),
(10,1010,'C002','2025-01-20','Delivered',3750);

-- ============================================================
-- SECTION 2: DATA QUALITY CHECKS
-- Run these on any new dataset before client delivery
-- ============================================================

-- Check 1: Total record count intake check
SELECT COUNT(*) AS total_records
FROM customers;

-- Check 2: Duplicate customer ID detection
SELECT customer_id, COUNT(*) AS times_appearing
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check 3: Null check across all key fields
SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
    SUM(CASE WHEN zip_code IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) AS null_segment,
    SUM(CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END) AS null_campaign_id
FROM customers;

-- Check 4: Segment consistency check
SELECT segment, COUNT(*) AS record_count
FROM customers
GROUP BY segment
ORDER BY segment;

-- Check 5: Outlier detection on spend amount
SELECT customer_id,
       first_name,
       last_name,
       spend_amount,
       ROUND((SELECT AVG(spend_amount) FROM customers), 2) AS avg_spend,
       ROUND(spend_amount - (SELECT AVG(spend_amount) FROM customers), 2) AS difference_from_avg
FROM customers
WHERE spend_amount > (SELECT AVG(spend_amount) FROM customers) * 3
ORDER BY spend_amount DESC;

-- Check 6: Reconciliation query
SELECT c.customer_id,
       c.first_name,
       c.last_name,
       c.campaign_id,
       c.record_date
FROM customers c
LEFT JOIN deliveries d
ON c.customer_id = d.customer_id
WHERE d.customer_id IS NULL;

-- ============================================================
-- SECTION 3: DECISION SUPPORT QUERIES
-- ============================================================

-- Query 1: Response rate by segment
SELECT segment,
       COUNT(*) AS total_records,
       SUM(response_flag) AS total_responses,
       ROUND(AVG(response_flag) * 100, 1) AS response_rate_pct
FROM customers
GROUP BY segment
ORDER BY response_rate_pct DESC;

-- Query 2: Total and average spend by campaign
SELECT c.campaign_id,
       camp.campaign_name,
       camp.channel,
       COUNT(c.customer_id) AS total_customers,
       SUM(c.spend_amount) AS total_spend,
       ROUND(AVG(c.spend_amount), 2) AS avg_spend
FROM customers c
LEFT JOIN campaigns camp
ON c.campaign_id = camp.campaign_id
GROUP BY c.campaign_id, camp.campaign_name, camp.channel
ORDER BY total_spend DESC;

-- Query 3: SLA compliance by campaign
SELECT d.campaign_id,
       camp.campaign_name,
       COUNT(*) AS total_deliveries,
       SUM(CASE WHEN d.delivery_status = 'Delivered' THEN 1 ELSE 0 END) AS successful,
       SUM(CASE WHEN d.delivery_status = 'Failed' THEN 1 ELSE 0 END) AS failed,
       ROUND(SUM(CASE WHEN d.delivery_status = 'Delivered' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS sla_compliance_pct
FROM deliveries d
LEFT JOIN campaigns camp
ON d.campaign_id = camp.campaign_id
GROUP BY d.campaign_id, camp.campaign_name
ORDER BY sla_compliance_pct ASC;

-- Query 4: Full campaign performance summary
SELECT camp.campaign_name,
       camp.channel,
       camp.budget,
       COUNT(c.customer_id) AS total_customers,
       SUM(c.response_flag) AS total_responses,
       ROUND(AVG(c.response_flag) * 100, 1) AS response_rate_pct,
       ROUND(SUM(c.spend_amount), 2) AS total_spend,
       ROUND(AVG(c.spend_amount), 2) AS avg_spend_per_customer,
       ROUND(camp.budget - SUM(c.spend_amount), 2) AS budget_remaining
FROM campaigns camp
LEFT JOIN customers c
ON camp.campaign_id = c.campaign_id
GROUP BY camp.campaign_name, camp.channel, camp.budget
ORDER BY response_rate_pct DESC;
