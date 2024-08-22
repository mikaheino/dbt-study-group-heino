{{
    config(
        materialized='incremental',
        unique_key='o_orderkey'
    )
}}

select 
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    1 as a
  from {{ source('tpch','orders') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where o_orderdate > (select (max(o_orderdate)) from {{ this }} )

{% endif %}