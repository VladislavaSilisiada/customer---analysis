WITH account_metrics AS (
  --розрахунок данних для акаунт в розрізі країн та часу
    SELECT
        s.date,
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
         COUNT(DISTINCT a.account_id) AS account_cnt
    FROM `DA.account_session` a
    JOIN `DA.session_params` sp
     ON a.ga_session_id = sp.ga_session_id
     JOIN `DA.account` acc
    ON a.account_id = acc.id
    JOIN `DA.session` s
     ON sp.ga_session_id = s.ga_session_id
    GROUP BY 1, 2, 3, 4, 5
),
email_metrics AS (
  ---розрахунок данних для емейл метрик за тим же розрізом. дату знаходимо за допомогою інтервалу відправлень
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT o.id_message) AS open_msg,
        COUNT(DISTINCT v.id_message) AS visit_msg
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` o
    ON es.id_message = o.id_message
    LEFT JOIN `DA.email_visit` v
    ON v.id_message = es.id_message
   JOIN `DA.account_session` accs
     ON accs.account_id = es.id_account
    JOIN `DA.session` s
     ON accs.ga_session_id = s.ga_session_id
     JOIN `DA.session_params` sp
     ON sp.ga_session_id = s.ga_session_id
     JOIN `DA.account` acc
    ON acc.id = es.id_account  
    GROUP BY 1, 2, 3, 4, 5
),
combined_data AS (
  --комбінуємо метрики юніоном
    SELECT date, country, send_interval, is_verified, is_unsubscribed, account_cnt, 0 AS sent_msg, 0 AS open_msg, 0 AS visit_msg
    FROM account_metrics
    UNION ALL
    SELECT date, country,send_interval, is_verified, is_unsubscribed, 0 AS account_cnt, sent_msg, open_msg, visit_msg
    FROM email_metrics
),
final_data AS (
  --групуємо данні
    SELECT
        date,
         country,
        SUM(account_cnt) AS account_cnt,
         SUM(sent_msg) AS sent_msg,
          SUM(open_msg) AS open_msg,
         SUM(visit_msg) AS visit_msg,
         SUM(sent_msg) AS  total_country_sent_cnt,
          send_interval,    
         is_verified,        
         is_unsubscribed
    FROM combined_data
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
), total as ( --знаходимо тотал значення
SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
    SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM final_data
), rank as ( ---ранг для країн
    SELECT
     date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
    DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM total
)---фінальний запит
SELECT  
 date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  sent_msg,
  open_msg,
  visit_msg,
  total_country_account_cnt,
  total_country_sent_cnt,
  rank_total_country_account_cnt,
  rank_total_country_sent_cnt
FROM rank
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10 ORDER BY date, total_country_account_cnt desc
