-- name: test_agg_state_hll_sketch

CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
PARTITION BY (province, dt) 
DISTRIBUTED BY RANDOM;

INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');

CREATE TABLE test_hll_sketch (
  province VARCHAR(64),
  dt VARCHAR(10),
  hll_id agg_state<approx_count_distinct_hll_sketch(varchar not null)> agg_state_union,
  hll_province agg_state<approx_count_distinct_hll_sketch(varchar)> agg_state_union,
  hll_age agg_state<approx_count_distinct_hll_sketch(varchar)> agg_state_union,
  hll_dt agg_state<approx_count_distinct_hll_sketch(varchar not null)> agg_state_union
)
AGGREGATE KEY(province, dt)
PARTITION BY (province, dt) 
DISTRIBUTED BY RANDOM;

-- basic test for empty table
select id, province, from_binary(approx_count_distinct_hll_sketch_state(id), 'hex') from t1 order by 1, 2;
select id, province, from_binary(approx_count_distinct_hll_sketch_state(province), 'hex') from t1 order by 1, 2;
select id, province, from_binary(approx_count_distinct_hll_sketch_state(age), 'hex') from t1 order by 1, 2;
select id, province, from_binary(approx_count_distinct_hll_sketch_state(dt), 'hex') from t1 order by 1, 2;

select approx_count_distinct_hll_sketch_merge(hll_province) from test_hll_sketch;
select approx_count_distinct_hll_sketch_merge(hll_province) from test_hll_sketch group by dt order by 1;

-- first insert & test result
insert into test_hll_sketch select id, province, approx_count_distinct_hll_sketch_state(id), 
    approx_count_distinct_hll_sketch_state(province),
    approx_count_distinct_hll_sketch_state(age),
    approx_count_distinct_hll_sketch_state(dt) from t1;
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch;
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch group by dt order by 1;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_hll_sketch select id, province, approx_count_distinct_hll_sketch_state(id), 
    approx_count_distinct_hll_sketch_state(province),
    approx_count_distinct_hll_sketch_state(age),
    approx_count_distinct_hll_sketch_state(dt) from t1;
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch;
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch group by dt order by 1;
