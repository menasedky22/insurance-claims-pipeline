#select top 5 customers

SELECT 
    c.customer_id,
    SUM(c.claim_amount) AS total_claims
FROM claims c
JOIN policies p 
    ON c.policy_id = p.policy_id
WHERE c.claim_date BETWEEN p.start_date AND p.end_date
GROUP BY c.customer_id
ORDER BY total_claims DESC
LIMIT 5;
________________________________________________________________



#PostgreSQL

CREATE DATABASE insurance;

-- الاتصال بالقاعدة
\c insurance

-- إنشاء جدول Policies
CREATE TABLE policies (
    policy_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    policy_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    premium_amount NUMERIC
);

-- إنشاء جدول Claims مع عمود جديد invalid_claim
CREATE TABLE claims (
    claim_id SERIAL PRIMARY KEY,
    policy_id INT REFERENCES policies(policy_id),
    customer_id INT NOT NULL,
    claim_type VARCHAR(50),
    claim_amount NUMERIC,
    claim_date DATE,
    status VARCHAR(20),
    invalid_claim BOOLEAN
);


#Load data from CSV to postgreSQL

\copy policies(policy_id, customer_id, policy_type, start_date, end_date, premium_amount) FROM 'C:/Users/adam/roboAssesment/policies.csv' DELIMITER ',' CSV HEADER;


\copy claims(claim_id, policy_id, customer_id, claim_type, claim_amount, claim_date, status, invalid_claim) FROM 'C:/Users/adam/roboAssesment/claims_cleaned.csv' DELIMITER ',' CSV HEADER;


