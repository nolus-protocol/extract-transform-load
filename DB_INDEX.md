CREATE INDEX idx_from ON raw_message("from");
CREATE INDEX idx_to ON raw_message("to");
CREATE INDEX idx_auth ON subscription("auth");