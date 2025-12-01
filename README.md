# customer---analysis
This repository contains SQL code designed for analyzing user behavior and measuring email engagement metrics. The query calculates key metrics related to user accounts and email interactions, segmented by date, country, and specific account attributes like send interval, verification status, and unsubscription status.

The primary goal of this query is to combine two distinct sets of data—account-level sessions/activity and email engagement (sends, opens, visits)—to provide a comprehensive view of user behavior.
The output dataset is aggregated daily at the country level, enriched with account feature flags, and includes a ranking system to identify the top 10 countries based on both total accounts and total emails sent.

The analysis is performed using a series of Common Table Expressions (CTEs), each serving a specific purpose:

**1. account_metrics**
Purpose: Calculates the count of unique accounts (account_cnt) per day, country, and feature set (send interval, verified, unsubscribed).
Data Sources: Joins DA.account_session, DA.session_params, DA.account, and DA.session.

**2. email_metrics**
Purpose: Calculates email engagement metrics: sent messages (sent_msg), opened messages (open_msg), and visits from emails (visit_msg).
Key Logic: The date for email metrics is calculated using DATE_ADD(s.date, INTERVAL es.sent_date DAY) to align with the correct sending date.
Data Sources: Uses DA.email_sent with LEFT JOINs to DA.email_open and DA.email_visit, and joins back to session and account tables to get country and account features.

**3. combined_data**
Purpose: Unions the results from account_metrics and email_metrics to consolidate all raw metrics into a single dataset.
Note: Zero values are assigned to metrics not present in the respective CTE (e.g., account_cnt is 0 in the email part).

**4. final_data**
Purpose: Aggregates the combined_data to sum the individual metrics (account_cnt, sent_msg, open_msg, visit_msg) at the final desired granularity (date, country, and account features).

**5. total**
Purpose: Calculates total metrics per country across all dates and feature combinations using Window Functions.
total_country_account_cnt: Total accounts per country.
total_country_sent_cnt: Total messages sent per country.

**6. rank**
Purpose: Ranks the countries based on the total metrics calculated in the total CTE using the DENSE_RANK() window function.
rank_total_country_account_cnt
rank_total_country_sent_cnt

**7. Final Query**
Purpose: Selects all calculated metrics and filtering the results to include only the Top 10 countries by either total accounts or total emails sent.

The full SQL script is provided below: [`sql/customer.sql`](./sql/customer.sql).

Filter Condition: WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10

Ordering: The final result is ordered by date and total_country_account_cnt descending.
