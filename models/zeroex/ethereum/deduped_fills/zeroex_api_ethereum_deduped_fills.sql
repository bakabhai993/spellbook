
WITH fills_with_tx_fill_number
AS
(
    SELECT   row_number() OVER ( partition BY tx_hash ORDER BY evt_index ASC ) AS tx_fill_number
           , *
      FROM zeroex_ethereum.fills
    WHERE 1=1
    -- AND tx_hash IN ( 
    --                  '0xfebc6867d857bf727e1d9f5e7ef58bf2c562c64015e1901c2524652fedea513a'
    --                 ,'0x27f1de5f9eef84e3ea5a92d8802162e7ef6c5a0298fee2251385cbd805a0e04c'
    --                 ,'0x01c555264a5b79956b28e00c9392ff4b397e43f7abad76d33c2630317c933a01'
    --                 )
    AND block_date = '2022-08-09'
    AND swap_flag = 1
)
, fills_first_last
AS
(
    SELECT  CASE WHEN a.taker_token = c.maker_token   AND a.taker_token_amount_raw = c.maker_token_amount_raw   THEN 0 ELSE 1 END AS taker_consider_flag--from
          , CASE WHEN a.maker_token   = b.taker_token AND a.maker_token_amount_raw   = b.taker_token_amount_raw THEN 0 ELSE 1 END AS maker_consider_flag
          , SUM(CASE WHEN a.maker_token   = b.taker_token AND a.maker_token_amount_raw  = b.taker_token_amount_raw THEN 0 ELSE 1 END) OVER(PARTITION BY a.tx_hash ORDER BY a.evt_index DESC) hop_count
          , a.*
    
      FROM  fills_with_tx_fill_number a 
            LEFT JOIN fills_with_tx_fill_number b ON (a.tx_hash = b.tx_hash AND a.tx_fill_number = b.tx_fill_number - 1)
            LEFT JOIN fills_with_tx_fill_number c ON (a.tx_hash = c.tx_hash AND a.tx_fill_number = c.tx_fill_number + 1)    
)    
-- SELECT * 
--  FROM fills_first_last
-- ORDER BY evt_index ASC
, deduped_bridge_fills
AS
(
    SELECT 
              tx_hash
            , MAX(evt_index)                                                               AS evt_index
            , MAX(affiliate_address)                                                       AS affiliate_address
            , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token END )       AS taker_token
            , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token END )       AS maker_token
            , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token_amount END)   AS taker_token_amount
            , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token_amount END)   AS maker_token_amount
            , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token_amount_raw END)   AS taker_token_amount_raw
            , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token_amount_raw END)   AS maker_token_amount_raw
            , COUNT(*)                                                                     AS fills_within
    FROM fills_first_last a
    GROUP BY tx_hash,hop_count             
)
SELECT  'ethereum' AS blockchain
      , '0x API' AS project 
      , '' AS version
      , a.block_date
      , a.block_time
      , COALESCE(c.symbol,b.taker_token) AS token_bought_symbol
      , COALESCE(d.symbol,b.maker_token) AS token_sold_symbol
      , CASE WHEN lower(c.symbol) > lower(d.symbol) then concat(d.symbol, '-', c.symbol)
             ELSE concat(c.symbol, '-', d.symbol)
             END AS token_pair
      , b.taker_token_amount AS token_bought_amount
      , b.maker_token_amount AS token_sold_amount
      , b.taker_token_amount_raw AS token_bought_amount_raw
      , b.maker_token_amount_raw AS token_sold_amount_raw    
      , a.volume_usd AS amount_usd
      , b.taker_token AS token_bought_address
      , b.maker_token AS token_sold_address
      , a.taker
      , a.maker
      , a.affiliate_address AS project_contract_address
      , a.tx_hash
      , e.tx_from
      , e.tx_to
      , '' AS trace_address
      , b.evt_index
      , '' AS unique_trade_id
  FROM fills_with_tx_fill_number a 
    JOIN deduped_bridge_fills b ON (a.tx_hash = b.tx_hash AND a.evt_index = b.evt_index)
    LEFT JOIN tokens.erc20 c ON (c.contract_address = b.taker_token AND c.blockchain = 'ethereum')
    LEFT JOIN tokens.erc20 d ON (d.contract_address = b.maker_token AND c.blockchain = 'ethereum')
    LEFT JOIN ( SELECT from AS tx_from, to AS tx_to, hash AS tx_hash  FROM ethereum.transactions ) e ON (a.tx_hash = e.tx_hash)
ORDER BY  a.tx_hash,a.evt_index
;
--token bought is a
--token sold is b
--a taker
--b maker
--SELECT * FROM dex_aggregator.trades LIMIT 5

--SELECT * FROM dex_aggregator.trades LIMIT 5
--token bought is a is taker
--token sold is b is maker
--SELECT * FROM tokens.erc20 LIMIT 5