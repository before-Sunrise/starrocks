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
-- result:
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');
-- result:
-- !result
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
-- result:
-- !result
select id, province, from_binary(approx_count_distinct_hll_sketch_state(id), 'hex') from t1 order by 1, 2;
-- result:
1	a	020107110308010414F3D005
2	b	020107110308010407C5FB05
3	None	020107110308010453111E07
-- !result
select id, province, from_binary(approx_count_distinct_hll_sketch_state(province), 'hex') from t1 order by 1, 2;
-- result:
1	a	02010711030801046E24FA0C
2	b	02010711030801041665480A
3	None	None
-- !result
select id, province, from_binary(approx_count_distinct_hll_sketch_state(age), 'hex') from t1 order by 1, 2;
-- result:
1	a	020107110308010475BABD06
2	b	020107110308010475BABD06
3	None	None
-- !result
select id, province, from_binary(approx_count_distinct_hll_sketch_state(dt), 'hex') from t1 order by 1, 2;
-- result:
1	a	020107110308010429BD2A07
2	b	0201071103080104A8181106
3	None	020107110308010458669907
-- !result
select approx_count_distinct_hll_sketch_merge(hll_province) from test_hll_sketch;
-- result:
None
-- !result
select approx_count_distinct_hll_sketch_merge(hll_province) from test_hll_sketch group by dt order by 1;
-- result:
-- !result
insert into test_hll_sketch select id, province, approx_count_distinct_hll_sketch_state(id), 
    approx_count_distinct_hll_sketch_state(province),
    approx_count_distinct_hll_sketch_state(age),
    approx_count_distinct_hll_sketch_state(dt) from t1;
-- result:
-- !result
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch;
-- result:
E: (1064, "Getting analyzing error at line 2, column 4. Detail message: '`test_db_905bd6ae499011ef8c21f59689b429b3`.`test_hll_sketch`.`dt`' must be an aggregate expression or appear in GROUP BY clause.")
-- !result
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch group by dt order by 1;
-- result:
None	1	None	None	1
a	1	1	1	1
b	1	1	1	1
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
insert into test_hll_sketch select id, province, approx_count_distinct_hll_sketch_state(id), 
    approx_count_distinct_hll_sketch_state(province),
    approx_count_distinct_hll_sketch_state(age),
    approx_count_distinct_hll_sketch_state(dt) from t1;
-- result:
-- !result
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch;
-- result:
E: (1064, "Getting analyzing error at line 2, column 4. Detail message: '`test_db_905bd6ae499011ef8c21f59689b429b3`.`test_hll_sketch`.`dt`' must be an aggregate expression or appear in GROUP BY clause.")
-- !result
select 
    dt,
    approx_count_distinct_hll_sketch_merge(hll_id),
    approx_count_distinct_hll_sketch_merge(hll_province),
    approx_count_distinct_hll_sketch_merge(hll_age),
    approx_count_distinct_hll_sketch_merge(hll_dt) from test_hll_sketch group by dt order by 1;
-- result:
None	2	None	None	1
a	1	1	1	1
b	1	1	1	1
c	1	1	1	1
-- !result