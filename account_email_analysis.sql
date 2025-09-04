CREATE OR REPLACE VIEW Students.zhyrosh_module_task AS (
-- обраховуємо згруповані метрики для імейлів
with message_metrics as (
  SELECT DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date, country, ac.send_interval, ac.is_verified, ac.is_unsubscribed, 0 as account_cnt,
  COUNT(DISTINCT es.id_message) AS sent_msg, COUNT(DISTINCT eo.id_message) AS open_msg, COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM `data-analytics-mate.DA.email_sent` es
  LEFT JOIN `data-analytics-mate.DA.email_visit` ev USING(id_message)
  LEFT JOIN `data-analytics-mate.DA.email_open` eo USING (id_message)
  LEFT JOIN
    `data-analytics-mate.DA.account_session` acs
  ON
    es.id_account = acs.account_id
  JOIN `data-analytics-mate.DA.account` ac
    ON es.id_account=ac.id
  JOIN
    `data-analytics-mate.DA.session` s
  ON
    acs.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp
  ON acs.ga_session_id=sp.ga_session_id
GROUP BY 1, 2, 3, 4, 5
),
-- обраховуємо згруповані метрики для аккаунтів
account_metrics AS(
  SELECT s.date, country, ac.send_interval, ac.is_verified, ac.is_unsubscribed, COUNT(DISTINCT acs.account_id) AS account_cnt, 0 as sent_msg, 0 as open_msg, 0 as visit_msg
  FROM  `data-analytics-mate.DA.account_session` acs
  JOIN `data-analytics-mate.DA.account` ac
    ON acs.account_id=ac.id
  JOIN
    `data-analytics-mate.DA.session` s
  ON
    acs.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp
  ON acs.ga_session_id=sp.ga_session_id
GROUP BY 1, 2, 3, 4, 5
-- об'єднуємо результати
), union_metrics as(
  SELECT * from message_metrics
  UNION ALL
  SELECT * FROM account_metrics
),
-- сумуємо агреговані поля для уникнення нулів
result AS (
  SELECT date, country, send_interval, is_verified, is_unsubscribed, SUM(account_cnt) AS account_cnt, SUM(sent_msg) AS sent_msg, SUM(open_msg) AS open_msg, SUM(visit_msg) AS visit_msg
  FROM union_metrics
  GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),
-- знаходимо потрібні метрики в розрізі країн
final as (
SELECT *, SUM(account_cnt) over(partition by country) AS total_country_account_cnt, SUM(sent_msg) over(partition by country) AS total_country_sent_cnt
FROM result ),
-- присвоюємо ранг
ranking as (
SELECT *, DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC ) AS rank_total_country_account_cnt,
DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC ) AS rank_total_country_sent_cnt
from final)


SELECT * FROM ranking
WHERE rank_total_country_account_cnt <= 10
   OR rank_total_country_sent_cnt <= 10)

SELECT *
FROM Students.zhyrosh_module_task
