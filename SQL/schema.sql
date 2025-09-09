set search_path to public;

-- ===== K 線（日） =====
create table if not exists futures_candles_1d (
  exchange   text not null,
  symbol     text not null,
  ts_utc     timestamptz not null,
  date_utc   date not null,
  open numeric, high numeric, low numeric, close numeric, volume_usd numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_futures_candles_1d_date on futures_candles_1d(date_utc);
create index if not exists ix_futures_candles_1d_ts   on futures_candles_1d(ts_utc);

create table if not exists spot_candles_1d (
  exchange   text not null,
  symbol     text not null,
  ts_utc     timestamptz not null,
  date_utc   date not null,
  open numeric, high numeric, low numeric, close numeric, volume_usd numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_spot_candles_1d_date on spot_candles_1d(date_utc);
create index if not exists ix_spot_candles_1d_ts   on spot_candles_1d(ts_utc);

-- ===== OI（聚合、穩定幣、幣本位）=====
create table if not exists futures_oi_agg_1d (
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  open numeric, high numeric, low numeric, close numeric,
  unit text default 'usd',
  primary key (symbol, ts_utc, unit)
);
create index if not exists ix_oiagg1d_date on futures_oi_agg_1d(date_utc);
create index if not exists ix_oiagg1d_ts   on futures_oi_agg_1d(ts_utc);

create table if not exists futures_oi_stablecoin_1d (
  exchange_list text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  open numeric, high numeric, low numeric, close numeric,
  primary key (exchange_list, symbol, ts_utc)
);
create index if not exists ix_oistable1d_date on futures_oi_stablecoin_1d(date_utc);
create index if not exists ix_oistable1d_ts   on futures_oi_stablecoin_1d(ts_utc);

create table if not exists futures_oi_coin_margin_1d (
  exchange_list text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  open numeric, high numeric, low numeric, close numeric,
  primary key (exchange_list, symbol, ts_utc)
);
create index if not exists ix_oicoinm1d_date on futures_oi_coin_margin_1d(date_utc);
create index if not exists ix_oicoinm1d_ts   on futures_oi_coin_margin_1d(ts_utc);

-- ===== 資金費率（日）=====
create table if not exists funding_oi_weight_1d (
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  open numeric, high numeric, low numeric, close numeric,
  primary key (symbol, ts_utc)
);
create index if not exists ix_funding_oiw_date on funding_oi_weight_1d(date_utc);
create index if not exists ix_funding_oiw_ts   on funding_oi_weight_1d(ts_utc);

create table if not exists funding_vol_weight_1d (
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  open numeric, high numeric, low numeric, close numeric,
  primary key (symbol, ts_utc)
);
create index if not exists ix_funding_volw_date on funding_vol_weight_1d(date_utc);
create index if not exists ix_funding_volw_ts   on funding_vol_weight_1d(ts_utc);

-- ===== 多空比（日）=====
create table if not exists long_short_global_1d (
  exchange text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  long_percent numeric, short_percent numeric, long_short_ratio numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_lsg1d_date on long_short_global_1d(date_utc);
create index if not exists ix_lsg1d_ts   on long_short_global_1d(ts_utc);

create table if not exists long_short_top_accounts_1d (
  exchange text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  long_percent numeric, short_percent numeric, long_short_ratio numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_lsta1d_date on long_short_top_accounts_1d(date_utc);
create index if not exists ix_lsta1d_ts   on long_short_top_accounts_1d(ts_utc);

create table if not exists long_short_top_positions_1d (
  exchange text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  long_percent numeric, short_percent numeric, long_short_ratio numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_lstp1d_date on long_short_top_positions_1d(date_utc);
create index if not exists ix_lstp1d_ts   on long_short_top_positions_1d(ts_utc);

-- ===== 爆倉、委買委賣、主動買賣（日）=====
create table if not exists liquidation_agg_1d (
  exchange_list text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  long_liq_usd numeric, short_liq_usd numeric,
  primary key (exchange_list, symbol, ts_utc)
);
create index if not exists ix_liqagg1d_date on liquidation_agg_1d(date_utc);
create index if not exists ix_liqagg1d_ts   on liquidation_agg_1d(ts_utc);

drop table if exists orderbook_agg_futures_1d;
create table orderbook_agg_futures_1d (
  exchange_list text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  bids_usd numeric, bids_qty numeric, asks_usd numeric, asks_qty numeric,
  range_pct numeric not null default 0,
  primary key (exchange_list, symbol, ts_utc, range_pct)
);
create index if not exists ix_obagg1d_date on orderbook_agg_futures_1d(date_utc);
create index if not exists ix_obagg1d_ts   on orderbook_agg_futures_1d(ts_utc);

create table if not exists taker_vol_agg_futures_1d (
  exchange_list text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  buy_vol_usd numeric, sell_vol_usd numeric,
  primary key (exchange_list, symbol, ts_utc)
);
create index if not exists ix_takeragg1d_date on taker_vol_agg_futures_1d(date_utc);
create index if not exists ix_takeragg1d_ts   on taker_vol_agg_futures_1d(ts_utc);

-- ===== ETF（日，原本就是 date_utc，保留）=====
create table if not exists etf_bitcoin_flow_1d (
  date_utc date primary key,
  total_flow_usd numeric, price_usd numeric, details jsonb
);
create table if not exists etf_bitcoin_net_assets_1d (
  date_utc date primary key,
  net_assets_usd numeric, change_usd numeric, price_usd numeric
);
create table if not exists etf_premium_discount_1d (
  date_utc date not null, ticker text not null,
  nav_usd numeric, market_price_usd numeric, premium_discount numeric,
  primary key (date_utc, ticker)
);
create table if not exists hk_etf_flow_1d (
  date_utc date primary key,
  total_flow_usd numeric, price_usd numeric, details jsonb
);

-- ===== 外部指標（日）=====
create table if not exists coinbase_premium_index_1d (
  ts_utc timestamptz primary key,
  date_utc date not null,
  premium_usd numeric, premium_rate numeric
);
create index if not exists ix_cpi1d_date on coinbase_premium_index_1d(date_utc);

create table if not exists bitfinex_margin_long_short_1d (
  symbol text not null,
  ts_utc timestamptz not null,
  date_utc date not null,
  long_qty numeric, short_qty numeric,
  primary key (symbol, ts_utc)
);
create index if not exists ix_bfxls1d_date on bitfinex_margin_long_short_1d(date_utc);
create index if not exists ix_bfxls1d_ts   on bitfinex_margin_long_short_1d(ts_utc);

create table if not exists borrow_interest_rate_1d (
  exchange text not null,
  symbol   text not null,
  ts_utc   timestamptz not null,
  date_utc date not null,
  interest_rate numeric,
  primary key (exchange, symbol, ts_utc)
);
create index if not exists ix_borrowir1d_date on borrow_interest_rate_1d(date_utc);
create index if not exists ix_borrowir1d_ts   on borrow_interest_rate_1d(ts_utc);

create table if not exists idx_puell_multiple_daily (
  date_utc date primary key, price numeric, puell_multiple numeric
);
create table if not exists idx_stock_to_flow_daily (
  date_utc date primary key, price numeric, next_halving int
);
create table if not exists idx_pi_cycle_daily (
  date_utc date primary key, price numeric, ma_110 numeric, ma_350_x2 numeric
);

-- ===== 通用觸發器：維護 date_utc = UTC( ts_utc )::date =====
create or replace function public._set_date_utc()
returns trigger
language plpgsql
as $$
begin
  if NEW.ts_utc is null then
    NEW.date_utc := null;
  else
    NEW.date_utc := (NEW.ts_utc at time zone 'UTC')::date;
  end if;
  return NEW;
end $$;

do $$
declare
  t text;
  ts_tables text[] := array[
    'futures_candles_1d',
    'spot_candles_1d',
    'futures_oi_agg_1d',
    'futures_oi_stablecoin_1d',
    'futures_oi_coin_margin_1d',
    'funding_oi_weight_1d',
    'funding_vol_weight_1d',
    'long_short_global_1d',
    'long_short_top_accounts_1d',
    'long_short_top_positions_1d',
    'liquidation_agg_1d',
    'orderbook_agg_futures_1d',
    'taker_vol_agg_futures_1d',
    'coinbase_premium_index_1d',
    'bitfinex_margin_long_short_1d',
    'borrow_interest_rate_1d'
  ];
begin
  foreach t in array ts_tables loop
    execute format('drop trigger if exists trg_set_date_utc on %I', t);
    execute format(
      'create trigger trg_set_date_utc
         before insert or update of ts_utc on %I
       for each row execute function public._set_date_utc()', t);
  end loop;
end
$$ language plpgsql;
