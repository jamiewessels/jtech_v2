from google.cloud import bigquery
import pandas as pd



query = """
              with user_deeplink_clicks as(
                        select e.user_id
                            , package_name
                            , lower(platform) as platform
                            , created_at as click_at
                            , lax_string(publisher_parameters['cs_iap_product_id']) product_id --change to product_id
                            , lax_string(publisher_parameters['cadence_group']) cadence_group
                        from `prj-cs-shared-public-a17f.cs_analytics.analytics_events_permanent` e
                        join (select distinct user_id from `prj-cs-shared-public-a17f.cs_analytics.analytics_events_permanent` where date(created_at)>=current_date - 120 and event_name = 'iap_purchase' and environment = 'production') i
                        on e.user_id = i.user_id
                        where date(created_at)>=current_date - 90
                            and event_name = 'deeplink_click'
                            and environment = 'production'
                            and (publisher_parameters.browserType is null or lower(string(publisher_parameters.browserType))<>'bot')
                            and lax_string(publisher_parameters['cs_iap_product_id']) is not null
                    ),

                user_product_purchases as (
                    select id as purchase_id
                        , user_id
                        , package_name
                        , lower(platform) as platform
                        , created_at as purchase_at
                        , lax_string(publisher_parameters['cs_iap_product_id']) product_id --change to product_id
                    from `prj-cs-shared-public-a17f.cs_analytics.analytics_events_permanent`
                    where date(created_at)>=current_date - 90
                        and event_name = 'iap_purchase'
                        and environment = 'production'
                        and lax_string(publisher_parameters['cs_iap_product_id']) is not null
                ),

                purchases_after_click_same_product as (
                    select p.product_id
                        , p.purchase_at
                        , p.package_name
                        , p.platform
                        , p.user_id
                        , c.click_at
                        , c.cadence_group
                        , row_number() over(partition by p.purchase_id order by timestamp_diff(p.purchase_at, c.click_at, SECOND)) as rn
                    from user_product_purchases p
                    join user_deeplink_clicks c on p.product_id = c.product_id and p.platform = c.platform and p.package_name = c.package_name and p.user_id = c.user_id
                        and p.purchase_at between c.click_at and (c.click_at + interval 24 hour)
                ),

                attributed_purchases as (
                    select package_name
                        , platform
                        , user_id
                        , product_id
                        , cadence_group
                    from purchases_after_click_same_product
                    where rn = 1
                )


          select user_id
              , package_name
              , lower(platform) as platform
              , product_id
              , 'click' as event_name
              , cadence_group
              , count(1) as count
          from user_deeplink_clicks
          where product_id is not null
              and platform is not null
              and product_id != 'null'
          group by 1, 2, 3, 4, 5, 6

          union all


          select userId as user_id
              , packageName as package_name
              , lower(platform) as platform
              , json_extract_scalar(metaInfo, '$.cs_iap_product_id')  product_id
              , 'sent' as event_name
              , json_extract_scalar(metaInfo, '$.cadenceGroup') cadence_group
              , count(1) as count
          from `prj-cs-shared-public-a17f.cs_analytics.outreach_events` o
          join (select distinct user_id from `prj-cs-shared-public-a17f.cs_analytics.analytics_events_permanent` where date(created_at)>=current_date - 120 and event_name = 'iap_purchase' and environment = 'production') i
              on userId = i.user_id
          where json_extract_scalar(metaInfo, '$.cs_iap_product_id') is not null
              and platform is not null
              and json_extract_scalar(metaInfo, '$.cs_iap_product_id') != 'null'
          group by 1, 2, 3, 4, 5, 6

          union all

          select user_id
              , package_name
              , platform
              , product_id
              , 'iap' as event_name
              , cadence_group
              , count(1) as cadence_stats
          from attributed_purchases
          where product_id is not null
              and product_id != 'null'
          group by 1, 2, 3, 4, 5, 6 
    """


def get_past_behavior_all(query=query):
    client = bigquery.Client()
    query_job = client.query(query)
    df = query_job.to_dataframe()

    return df