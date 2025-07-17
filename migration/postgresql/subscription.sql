CREATE TABLE IF NOT EXISTS subscription (
  active BOOLEAN DEFAULT true NOT NULL,
  address VARCHAR(44) NOT NULL,
  p256dh VARCHAR(87) NOT NULL,
  auth VARCHAR(22) NOT NULL,
  endpoint TEXT NOT NULL,
  expiration TIMESTAMPTZ DEFAULT NULL,
  ip VARCHAR(45),
  user_agent TEXT,
  PRIMARY KEY (address, p256dh, auth)
);