CREATE TABLE IF NOT EXISTS "raw_message" (
  "index" INT NOT NULL,
  "from" VARCHAR(64) NOT NULL,
  "to" VARCHAR(64) NOT NULL,
  "tx_hash" VARCHAR(64) NOT NULL,
  "type" VARCHAR(64) NOT NULL,
  "value" TEXT,
  "block" BIGINT NOT NULL,
  "fee_amount" DECIMAL(39, 0),
  "fee_denom" VARCHAR(20),
  "memo" TEXT,
  "timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("index", "tx_hash")
);