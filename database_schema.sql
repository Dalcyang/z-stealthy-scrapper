-- Sports Betting Odds Scraper Database Schema
-- Designed for Supabase PostgreSQL

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Bookmakers table
CREATE TABLE IF NOT EXISTS bookmakers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    website_url VARCHAR(200) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sport events table
CREATE TABLE IF NOT EXISTS sport_events (
    id SERIAL PRIMARY KEY,
    sport_type VARCHAR(50) NOT NULL,
    home_team VARCHAR(200) NOT NULL,
    away_team VARCHAR(200) NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    league VARCHAR(200) NOT NULL,
    country VARCHAR(100) DEFAULT 'South Africa',
    is_live BOOLEAN DEFAULT false,
    external_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_event_date CHECK (event_date > NOW() - INTERVAL '7 days')
);

-- Odds table
CREATE TABLE IF NOT EXISTS odds (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES sport_events(id) ON DELETE CASCADE,
    bookmaker_id INTEGER NOT NULL REFERENCES bookmakers(id) ON DELETE CASCADE,
    bet_type VARCHAR(50) NOT NULL,
    selection VARCHAR(200) NOT NULL,
    odds_decimal DECIMAL(10,3) NOT NULL CHECK (odds_decimal >= 1.001 AND odds_decimal <= 1000.0),
    odds_fractional VARCHAR(20),
    odds_american INTEGER,
    stake_limit DECIMAL(12,2),
    is_available BOOLEAN DEFAULT true,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Additional metadata
    market_id VARCHAR(100),
    original_data JSONB,
    confidence_score DECIMAL(3,2) DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    
    -- Constraints
    UNIQUE(event_id, bookmaker_id, bet_type, selection)
);

-- Arbitrage opportunities table
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES sport_events(id) ON DELETE CASCADE,
    sport_type VARCHAR(50) NOT NULL,
    home_team VARCHAR(200) NOT NULL,
    away_team VARCHAR(200) NOT NULL,
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Arbitrage details
    bet_type VARCHAR(50) NOT NULL,
    profit_percentage DECIMAL(8,4) NOT NULL CHECK (profit_percentage > 0),
    total_stake DECIMAL(12,2) NOT NULL CHECK (total_stake > 0),
    expected_profit DECIMAL(12,2) NOT NULL CHECK (expected_profit > 0),
    
    -- Odds involved in arbitrage (JSON array)
    odds_data JSONB NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT true,
    confidence_score DECIMAL(3,2) DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    
    -- Risk assessment
    risk_level VARCHAR(10) DEFAULT 'medium' CHECK (risk_level IN ('low', 'medium', 'high')),
    notes TEXT
);

-- Scraper performance logs
CREATE TABLE IF NOT EXISTS scraper_logs (
    id SERIAL PRIMARY KEY,
    bookmaker_id INTEGER REFERENCES bookmakers(id),
    sport_type VARCHAR(50),
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    events_found INTEGER DEFAULT 0,
    odds_extracted INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    performance_metrics JSONB,
    error_details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- System configuration table
CREATE TABLE IF NOT EXISTS system_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(100) NOT NULL UNIQUE,
    config_value JSONB NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sport_events_date ON sport_events(event_date);
CREATE INDEX IF NOT EXISTS idx_sport_events_teams ON sport_events(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_sport_events_sport_type ON sport_events(sport_type);
CREATE INDEX IF NOT EXISTS idx_sport_events_league ON sport_events(league);

CREATE INDEX IF NOT EXISTS idx_odds_event_id ON odds(event_id);
CREATE INDEX IF NOT EXISTS idx_odds_bookmaker_id ON odds(bookmaker_id);
CREATE INDEX IF NOT EXISTS idx_odds_bet_type ON odds(bet_type);
CREATE INDEX IF NOT EXISTS idx_odds_last_updated ON odds(last_updated);
CREATE INDEX IF NOT EXISTS idx_odds_available ON odds(is_available) WHERE is_available = true;
CREATE INDEX IF NOT EXISTS idx_odds_event_bookmaker ON odds(event_id, bookmaker_id);

CREATE INDEX IF NOT EXISTS idx_arbitrage_event_id ON arbitrage_opportunities(event_id);
CREATE INDEX IF NOT EXISTS idx_arbitrage_profit ON arbitrage_opportunities(profit_percentage DESC);
CREATE INDEX IF NOT EXISTS idx_arbitrage_active ON arbitrage_opportunities(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_arbitrage_expires ON arbitrage_opportunities(expires_at);
CREATE INDEX IF NOT EXISTS idx_arbitrage_created ON arbitrage_opportunities(created_at);

CREATE INDEX IF NOT EXISTS idx_scraper_logs_bookmaker ON scraper_logs(bookmaker_id);
CREATE INDEX IF NOT EXISTS idx_scraper_logs_start_time ON scraper_logs(start_time);
CREATE INDEX IF NOT EXISTS idx_scraper_logs_status ON scraper_logs(status);

-- Create GIN indexes for JSONB columns
CREATE INDEX IF NOT EXISTS idx_odds_original_data_gin ON odds USING GIN(original_data);
CREATE INDEX IF NOT EXISTS idx_arbitrage_odds_data_gin ON arbitrage_opportunities USING GIN(odds_data);

-- Create views for common queries
CREATE OR REPLACE VIEW v_latest_odds AS
SELECT DISTINCT ON (o.event_id, o.bookmaker_id, o.bet_type, o.selection) 
    o.*,
    se.home_team,
    se.away_team,
    se.event_date,
    se.league,
    b.display_name as bookmaker_name
FROM odds o
JOIN sport_events se ON o.event_id = se.id
JOIN bookmakers b ON o.bookmaker_id = b.id
WHERE o.is_available = true
    AND se.event_date > NOW()
ORDER BY o.event_id, o.bookmaker_id, o.bet_type, o.selection, o.last_updated DESC;

CREATE OR REPLACE VIEW v_active_arbitrage AS
SELECT 
    ao.*,
    se.league,
    EXTRACT(EPOCH FROM (ao.expires_at - NOW()))/3600 as hours_remaining
FROM arbitrage_opportunities ao
JOIN sport_events se ON ao.event_id = se.id
WHERE ao.is_active = true
    AND (ao.expires_at IS NULL OR ao.expires_at > NOW())
    AND ao.event_date > NOW()
ORDER BY ao.profit_percentage DESC;

CREATE OR REPLACE VIEW v_bookmaker_performance AS
SELECT 
    b.display_name,
    COUNT(sl.id) as total_scrapes,
    COUNT(CASE WHEN sl.status = 'completed' THEN 1 END) as successful_scrapes,
    COUNT(CASE WHEN sl.status = 'failed' THEN 1 END) as failed_scrapes,
    ROUND(AVG(sl.odds_extracted), 2) as avg_odds_per_scrape,
    ROUND(AVG(EXTRACT(EPOCH FROM (sl.end_time - sl.start_time))), 2) as avg_duration_seconds,
    MAX(sl.end_time) as last_successful_scrape
FROM bookmakers b
LEFT JOIN scraper_logs sl ON b.id = sl.bookmaker_id
WHERE sl.created_at > NOW() - INTERVAL '7 days'
GROUP BY b.id, b.display_name
ORDER BY successful_scrapes DESC;

-- Functions for data maintenance
CREATE OR REPLACE FUNCTION cleanup_old_odds(days_old INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM odds 
    WHERE last_updated < NOW() - INTERVAL '1 day' * days_old;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_expired_arbitrage()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE arbitrage_opportunities 
    SET is_active = false 
    WHERE is_active = true 
        AND expires_at < NOW();
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_bookmakers_updated_at
    BEFORE UPDATE ON bookmakers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sport_events_updated_at
    BEFORE UPDATE ON sport_events
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_system_config_updated_at
    BEFORE UPDATE ON system_config
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Insert default bookmakers
INSERT INTO bookmakers (name, display_name, website_url) 
VALUES 
    ('hollywoodbets', 'Hollywoodbets', 'https://www.hollywoodbets.net'),
    ('betway', 'Betway', 'https://www.betway.co.za'),
    ('supabets', 'Supabets', 'https://www.supabets.co.za'),
    ('playabets', 'Playabets', 'https://www.playabets.co.za'),
    ('playbets', 'Playbets', 'https://www.playbets.co.za'),
    ('yesbet', 'YesBet', 'https://www.yesbet.co.za')
ON CONFLICT (name) DO NOTHING;

-- Insert default system configuration
INSERT INTO system_config (config_key, config_value, description)
VALUES 
    ('scraper_settings', '{"delay_min": 1, "delay_max": 5, "max_concurrent": 3}', 'Global scraper settings'),
    ('arbitrage_settings', '{"min_profit_percentage": 1.0, "max_stake": 1000}', 'Arbitrage detection settings'),
    ('cleanup_settings', '{"odds_retention_days": 7, "logs_retention_days": 30}', 'Data cleanup settings')
ON CONFLICT (config_key) DO NOTHING;

-- Create RLS policies (if using Supabase Row Level Security)
-- Note: Adjust these based on your authentication requirements

-- Enable RLS on tables
ALTER TABLE bookmakers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sport_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE odds ENABLE ROW LEVEL SECURITY;
ALTER TABLE arbitrage_opportunities ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_config ENABLE ROW LEVEL SECURITY;

-- Create policies for read access (adjust based on your needs)
CREATE POLICY "Enable read access for all users" ON bookmakers FOR SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON sport_events FOR SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON odds FOR SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON arbitrage_opportunities FOR SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON scraper_logs FOR SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON system_config FOR SELECT USING (true);

-- Create policies for write access (restrict to service role)
CREATE POLICY "Enable insert for service role" ON sport_events FOR INSERT USING (auth.role() = 'service_role');
CREATE POLICY "Enable insert for service role" ON odds FOR INSERT USING (auth.role() = 'service_role');
CREATE POLICY "Enable insert for service role" ON arbitrage_opportunities FOR INSERT USING (auth.role() = 'service_role');
CREATE POLICY "Enable insert for service role" ON scraper_logs FOR INSERT USING (auth.role() = 'service_role');

CREATE POLICY "Enable update for service role" ON sport_events FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "Enable update for service role" ON odds FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "Enable update for service role" ON arbitrage_opportunities FOR UPDATE USING (auth.role() = 'service_role');
CREATE POLICY "Enable update for service role" ON scraper_logs FOR UPDATE USING (auth.role() = 'service_role');

-- Grant necessary permissions
GRANT USAGE ON SCHEMA public TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- Refresh views permissions
GRANT SELECT ON v_latest_odds TO anon, authenticated;
GRANT SELECT ON v_active_arbitrage TO anon, authenticated;
GRANT SELECT ON v_bookmaker_performance TO anon, authenticated;