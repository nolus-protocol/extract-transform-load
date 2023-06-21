CREATE TABLE IF NOT EXISTS "action_history" (
  "action_type" VARCHAR(1) NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("action_type", "created_at")
);